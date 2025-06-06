#!/bin/sh
#
#
# Enhanced Proxmox LVM Snapshot Manager with LXC Container Support
# 
# Prerequesties on all PVE nodes :
#  
# in etc/lvm/lvm.conf
#
# enable settings :
#
#         snapshot_autoextend_threshold = 70
#         snapshot_autoextend_percent = 20
#
# For cluster support:
#  - SSH key authentication between cluster nodes
#  - Same script deployed on all nodes
#
# Supports both QEMU VMs and LXC Containers
#
# Rewritten by Nico Schmidt (baGStube_Nico)
# E-Mail: nico.schmidt@ns-tech.cloud
# Follow my Socials: https://linktr.ee/bagstube_nico

# Global variables for cluster support
CLUSTER_MODE=""
CURRENT_NODE=""
CLUSTER_NODES=""
NON_INTERACTIVE="false"
FORCE_INTERACTIVE="false"
DEBUG_MODE="false"
CONTAINER_MODE="false"
SHOW_BANNER="true"

# Debug function
debug() {
    if [ "$DEBUG_MODE" = "true" ]; then
        echo "DEBUG: $*" >&2
    fi
}

# Show support message
show_support_message() {
    # Don't show banner if we're in a SSH session (remote execution)
    if [ -n "$SSH_CLIENT" ] || [ -n "$SSH_TTY" ] || [ "$TERM" = "dumb" ] || [ "$SHOW_BANNER" = "false" ]; then
        return
    fi
    
    echo "============================================================================"
    echo "  Enhanced Proxmox LVM Snapshot Manager"
    echo "  Remastered by Nico Schmidt (baGStube_Nico)"
    echo ""
    echo "  Supports: QEMU VMs and LXC Containers"
    echo ""
    echo "  Please consider supporting this script development:"
    echo "  💖 Ko-fi: ko-fi.com/bagstube_nico"
    echo "  🔗 Links: linktr.ee/bagstube_nico"
    echo "============================================================================"
    echo ""
}

# 
usage() {
    echo "Usage: $0 <action> <vmid/ctid> [<snapshotname>] [options]"
    echo "  action       : Action (list, create, delete, revert)"
    echo "  vmid/ctid    : VM ID or Container ID"
    echo "  snapshotname : snapshot name (mandatory for action create,delete,revert)"
    echo ""
    echo "Container/VM detection:"
    echo "  --container  : Force container mode (use pct commands)"
    echo "  --vm         : Force VM mode (use qm commands)"
    echo "  (auto-detect if not specified)"
    echo ""
    echo "Options for revert action:"
    echo "  --no-autostart : Do not start VM/CT after revert (starts by default)"
    echo "  --keep-snapshot : Keep snapshot after revert (default: ask user)"
    echo "  --delete-snapshot : Delete snapshot after revert"
    echo "  --non-interactive : Skip all prompts (for automated execution)"
    echo "  --interactive : Force interactive mode even for remote execution"
    echo ""
    echo "Cluster options:"
    echo "  --force-local : Force local-only operation (skip cluster coordination)"
    echo "  --cluster-sync : Force cluster synchronization even for local VMs/CTs"
    echo "  --setup-ssh : Setup SSH keys for cluster communication"
    echo ""
    echo "Debug options:"
    echo "  --debug : Enable debug output for troubleshooting"
    echo ""
    echo "Internal options (used by script):"
    echo "  --no-banner : Suppress support banner (used for remote execution)"
}

# Enhanced detection function with multiple methods and cluster support
detect_container_or_vm() {
    local id="$1"
    
    debug "Enhanced detection for ID: $id"
    
    # Method 1: Check local config files first (most reliable)
    debug "Method 1: Checking config files locally"
    if [ -f "/etc/pve/lxc/${id}.conf" ]; then
        debug "Found container config file: /etc/pve/lxc/${id}.conf"
        return 0  # Container
    fi
    
    if [ -f "/etc/pve/qemu-server/${id}.conf" ]; then
        debug "Found VM config file: /etc/pve/qemu-server/${id}.conf"
        return 1  # VM
    fi
    
    # Method 2: Try status commands locally
    debug "Method 2: Trying status commands locally"
    if pct status "$id" >/dev/null 2>&1; then
        debug "pct status succeeded for $id"
        return 0  # Container
    fi
    
    if qm status "$id" >/dev/null 2>&1; then
        debug "qm status succeeded for $id"
        return 1  # VM
    fi
    
    # Method 3: Check local lists
    debug "Method 3: Checking local instance lists"
    if pct list 2>/dev/null | grep -q "^[[:space:]]*$id[[:space:]]"; then
        debug "Found $id in pct list"
        return 0  # Container
    fi
    
    if qm list 2>/dev/null | grep -q "^[[:space:]]*$id[[:space:]]"; then
        debug "Found $id in qm list"
        return 1  # VM
    fi
    
    # Method 4: Use pvesh if available
    debug "Method 4: Trying pvesh cluster resources"
    if command -v pvesh >/dev/null 2>&1; then
        # Check for containers
        if pvesh get /cluster/resources --type lxc 2>/dev/null | grep -q "\"vmid\":$id"; then
            debug "Found $id in cluster LXC resources"
            return 0  # Container
        fi
        
        # Check for VMs
        if pvesh get /cluster/resources --type vm 2>/dev/null | grep -q "\"vmid\":$id"; then
            debug "Found $id in cluster VM resources"
            return 1  # VM
        fi
    fi
    
    # Method 5: Check other cluster nodes if in cluster mode
    if [ "$CLUSTER_MODE" = "true" ]; then
        debug "Method 5: Checking other cluster nodes"
        for node in $CLUSTER_NODES; do
            if [ "$node" != "$CURRENT_NODE" ]; then
                debug "Checking node $node for instance $id"
                if test_ssh_connection "$node"; then
                    # Check for container config
                    if ssh "root@${node}" "test -f /etc/pve/lxc/${id}.conf" 2>/dev/null; then
                        debug "Found container config on node $node"
                        return 0  # Container
                    fi
                    
                    # Check for VM config  
                    if ssh "root@${node}" "test -f /etc/pve/qemu-server/${id}.conf" 2>/dev/null; then
                        debug "Found VM config on node $node"
                        return 1  # VM
                    fi
                    
                    # Try status commands on remote node
                    if ssh "root@${node}" "pct status $id >/dev/null 2>&1"; then
                        debug "pct status succeeded on node $node"
                        return 0  # Container
                    fi
                    
                    if ssh "root@${node}" "qm status $id >/dev/null 2>&1"; then
                        debug "qm status succeeded on node $node"
                        return 1  # VM
                    fi
                    
                    # Try list commands on remote node
                    if ssh "root@${node}" "pct list 2>/dev/null | grep -q '^[[:space:]]*$id[[:space:]]'"; then
                        debug "Found $id in pct list on node $node"
                        return 0  # Container
                    fi
                    
                    if ssh "root@${node}" "qm list 2>/dev/null | grep -q '^[[:space:]]*$id[[:space:]]'"; then
                        debug "Found $id in qm list on node $node"
                        return 1  # VM
                    fi
                else
                    debug "Cannot connect to node $node via SSH"
                fi
            fi
        done
    fi
    
    # Method 6: Check for LVM volumes with naming patterns
    debug "Method 6: Checking LVM volumes for naming patterns"
    if lvs 2>/dev/null | grep -q "vm-${id}-disk"; then
        debug "Found LVM volumes matching vm-${id}-disk pattern"
        # Try to determine type from volume characteristics
        # This is a last resort and may not be 100% accurate
        local vm_volumes=$(lvs 2>/dev/null | grep "vm-${id}-disk" | head -1)
        if [ -n "$vm_volumes" ]; then
            debug "LVM volumes found, defaulting to VM (less risky assumption)"
            return 1  # Default to VM when unsure
        fi
    fi
    
    debug "All detection methods failed for ID: $id"
    return 2  # Unknown
}

# Improved function to show what was found during detection
show_detection_details() {
    local id="$1"
    
    echo "Detection details for ID $id:"
    echo "==================================="
    
    # Check config files
    echo "Config files:"
    if [ -f "/etc/pve/lxc/${id}.conf" ]; then
        echo "  ✓ Container config: /etc/pve/lxc/${id}.conf"
    else
        echo "  ✗ Container config: /etc/pve/lxc/${id}.conf"
    fi
    
    if [ -f "/etc/pve/qemu-server/${id}.conf" ]; then
        echo "  ✓ VM config: /etc/pve/qemu-server/${id}.conf"
    else
        echo "  ✗ VM config: /etc/pve/qemu-server/${id}.conf"
    fi
    
    # Check status commands
    echo "Status commands:"
    if pct status "$id" >/dev/null 2>&1; then
        echo "  ✓ pct status $id: $(pct status "$id" 2>/dev/null)"
    else
        echo "  ✗ pct status $id: failed"
    fi
    
    if qm status "$id" >/dev/null 2>&1; then
        echo "  ✓ qm status $id: $(qm status "$id" 2>/dev/null)"
    else
        echo "  ✗ qm status $id: failed"
    fi
    
    # Check lists
    echo "Instance lists:"
    if pct list 2>/dev/null | grep -q "^[[:space:]]*$id[[:space:]]"; then
        echo "  ✓ Found in pct list"
        pct list 2>/dev/null | grep "^[[:space:]]*$id[[:space:]]" | head -1
    else
        echo "  ✗ Not found in pct list"
    fi
    
    if qm list 2>/dev/null | grep -q "^[[:space:]]*$id[[:space:]]"; then
        echo "  ✓ Found in qm list"
        qm list 2>/dev/null | grep "^[[:space:]]*$id[[:space:]]" | head -1
    else
        echo "  ✗ Not found in qm list"
    fi
    
    # Check LVM volumes
    echo "LVM volumes:"
    local lvm_volumes=$(lvs 2>/dev/null | grep "vm-${id}-disk" || echo "none")
    echo "  LVM volumes: $lvm_volumes"
    
    echo "==================================="
}

# Get status of container or VM
get_status() {
    local id="$1"
    local is_container="$2"
    
    if [ "$is_container" = "true" ]; then
        debug "Getting container status for $id"
        pct status "$id" 2>/dev/null
    else
        debug "Getting VM status for $id"
        qm status "$id" 2>/dev/null
    fi
}

# Start container or VM
start_instance() {
    local id="$1"
    local is_container="$2"
    
    if [ "$is_container" = "true" ]; then
        debug "Starting container $id"
        echo "Starting container $id..."
        pct start "$id"
    else
        debug "Starting VM $id"
        echo "Starting VM $id..."
        qm start "$id"
    fi
}

# Stop container or VM
stop_instance() {
    local id="$1"
    local is_container="$2"
    
    if [ "$is_container" = "true" ]; then
        debug "Stopping container $id"
        echo "Stopping container $id..."
        pct stop "$id"
    else
        debug "Stopping VM $id"
        echo "Stopping VM $id..."
        qm stop "$id"
    fi
}

# Get config of container or VM
get_config() {
    local id="$1"
    local is_container="$2"
    
    if [ "$is_container" = "true" ]; then
        debug "Getting container config for $id"
        pct config "$id" 2>/dev/null
    else
        debug "Getting VM config for $id"
        qm config "$id" 2>/dev/null
    fi
}

# Setup SSH keys for cluster communication
setup_ssh_keys() {
    echo "Setting up SSH keys for cluster communication..."
    
    if [ ! -f ~/.ssh/id_rsa ]; then
        echo "Generating SSH key..."
        ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -N ""
    fi
    
    for node in $CLUSTER_NODES; do
        if [ "$node" != "$CURRENT_NODE" ]; then
            echo "Setting up SSH key for node: $node"
            ssh-copy-id "root@${node}"
            
            # Test connection
            if ssh -o ConnectTimeout=5 "root@${node}" "echo 'SSH connection successful'"; then
                echo "SSH connection to $node: SUCCESS"
            else
                echo "SSH connection to $node: FAILED"
            fi
        fi
    done
}

# Initialize cluster detection
init_cluster_support() {
    CURRENT_NODE=$(hostname)
    debug "Current node: $CURRENT_NODE"
    
    # Check if we're in a Proxmox cluster
    if [ -f /etc/pve/corosync.conf ] && command -v pvecm >/dev/null 2>&1; then
        CLUSTER_MODE="true"
        debug "Proxmox cluster configuration detected"
        
        # Get list of cluster nodes - improved parsing
        CLUSTER_NODES=$(pvecm nodes 2>/dev/null | awk '/^[[:space:]]*[0-9]+/ {print $3}' | grep -v "^$" | sort | uniq)
        if [ -z "$CLUSTER_NODES" ]; then
            debug "First method failed, trying fallback method"
            # Fallback method
            CLUSTER_NODES=$(pvecm status 2>/dev/null | grep "Name:" | awk '{print $2}' | grep -v "^$")
        fi
        if [ -z "$CLUSTER_NODES" ]; then
            debug "All methods failed, using current node only"
            # Another fallback - just use current node
            CLUSTER_NODES="$CURRENT_NODE"
        fi
        echo "Cluster mode detected. Nodes: $CLUSTER_NODES"
        debug "Cluster nodes detected: $CLUSTER_NODES"
    else
        CLUSTER_MODE="false"
        CLUSTER_NODES="$CURRENT_NODE"
        echo "Single node mode detected."
        debug "No cluster configuration found, single node mode"
    fi
}

# Check if SSH connection to node works
test_ssh_connection() {
    local node="$1"
    
    if [ "$node" = "$CURRENT_NODE" ]; then
        debug "SSH test skipped for local node: $node"
        return 0
    fi
    
    debug "Testing SSH connection to node: $node"
    ssh -o ConnectTimeout=5 -o BatchMode=yes "root@${node}" "echo 'SSH connection test'" >/dev/null 2>&1
    result=$?
    debug "SSH test result for $node: $result"
    return $result
}

# Get the node where a VM/Container is currently located
get_instance_node() {
    local id="$1"
    local is_container="$2"
    
    debug "Looking for instance $id (container: $is_container)..."
    
    # Method 1: Check locally first
    if [ "$is_container" = "true" ]; then
        if pct status "$id" >/dev/null 2>&1; then
            debug "Found container $id locally"
            echo "$CURRENT_NODE"
            return 0
        fi
        debug "Container $id not found locally"
    else
        if qm status "$id" >/dev/null 2>&1; then
            debug "Found VM $id locally"
            echo "$CURRENT_NODE"
            return 0
        fi
        debug "VM $id not found locally"
    fi
    
    # Method 2: Check if config exists in cluster and try to determine location
    config_path=""
    if [ "$is_container" = "true" ]; then
        config_path="/etc/pve/lxc/${id}.conf"
    else
        config_path="/etc/pve/qemu-server/${id}.conf"
    fi
    
    if [ -f "$config_path" ]; then
        debug "Config found: $config_path"
        
        # Try pvesh to get instance info
        if command -v pvesh >/dev/null 2>&1; then
            debug "Trying pvesh to locate instance $id"
            instance_node=""
            
            if [ "$is_container" = "true" ]; then
                instance_node=$(pvesh get /cluster/resources --type lxc 2>/dev/null | grep "\"vmid\":$id" | grep -o '"node":"[^"]*"' | cut -d'"' -f4 | head -1)
            else
                instance_node=$(pvesh get /cluster/resources --type vm 2>/dev/null | grep "\"vmid\":$id" | grep -o '"node":"[^"]*"' | cut -d'"' -f4 | head -1)
            fi
            
            if [ -n "$instance_node" ]; then
                debug "Found instance $id on node $instance_node via pvesh"
                echo "$instance_node"
                return 0
            fi
            debug "pvesh method failed for instance $id"
        fi
        
        # Try listing on current node
        debug "Trying local list for instance $id"
        instance_info=""
        if [ "$is_container" = "true" ]; then
            instance_info=$(pct list 2>/dev/null | grep "^[[:space:]]*$id[[:space:]]")
        else
            instance_info=$(qm list 2>/dev/null | grep "^[[:space:]]*$id[[:space:]]")
        fi
        
        if [ -n "$instance_info" ]; then
            debug "Instance found in local list: $instance_info"
            echo "$CURRENT_NODE"
            return 0
        fi
        debug "Local list method failed for instance $id"
    else
        debug "Config file not found: $config_path"
    fi
    
    # Method 3: If in cluster mode, check other nodes via SSH
    if [ "$CLUSTER_MODE" = "true" ]; then
        debug "Checking other cluster nodes for instance $id"
        for node in $CLUSTER_NODES; do
            if [ "$node" != "$CURRENT_NODE" ]; then
                debug "Checking node $node for instance $id..."
                if test_ssh_connection "$node"; then
                    debug "SSH connection successful, checking instance status on $node"
                    check_cmd=""
                    if [ "$is_container" = "true" ]; then
                        check_cmd="pct status $id >/dev/null 2>&1"
                    else
                        check_cmd="qm status $id >/dev/null 2>&1"
                    fi
                    
                    if ssh "root@${node}" "$check_cmd"; then
                        debug "Found instance $id on node $node"
                        echo "$node"
                        return 0
                    fi
                    debug "Instance $id not found on node $node"
                else
                    debug "Cannot connect to node $node via SSH"
                    echo "HINT: Run './snapshot.sh --setup-ssh' to configure SSH keys" >&2
                fi
            fi
        done
    fi
    
    debug "Instance $id not found anywhere"
    return 1
}

# Deploy script to remote node and execute with flexible interaction support
# Deploy script to remote node and execute with flexible interaction support
execute_remote_snapshot() {
    local node="$1"
    local action="$2"
    local id="$3"
    local snapshotname="$4"
    local extra_args="$5"
    
    script_name="snapshot_temp_$(date +%s).sh"
    remote_path="/tmp/$script_name"
    
    debug "Deploying script to node $node as $remote_path"
    echo "Deploying script to node $node and executing..."
    
    # Copy script to remote node
    debug "Copying script to $node:$remote_path"
    scp "$0" "root@${node}:${remote_path}" >/dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        debug "Script copied successfully to $node"
        
        # Add debug flag to remote execution if enabled
        debug_flag=""
        if [ "$DEBUG_MODE" = "true" ]; then
            debug_flag="--debug"
        fi
        
        # Add container flag if needed
        container_flag=""
        if [ "$CONTAINER_MODE" = "true" ]; then
            container_flag="--container"
        fi
        
        # Build complete arguments string with --no-banner first
        complete_args="--force-local --no-banner $debug_flag $container_flag $extra_args"
        
        # Determine execution mode
        if [ "$FORCE_INTERACTIVE" = "true" ]; then
            # Force interactive mode with terminal forwarding
            debug "Using forced interactive mode for remote execution"
            echo "Executing on remote node $node (interactive mode)..."
            ssh -t "root@${node}" "chmod +x $remote_path && $remote_path $action $id $snapshotname --interactive $complete_args; rm -f $remote_path"
        elif [ "$NON_INTERACTIVE" = "true" ]; then
            # Force non-interactive mode
            debug "Using forced non-interactive mode for remote execution"
            ssh "root@${node}" "chmod +x $remote_path && $remote_path $action $id $snapshotname --non-interactive $complete_args && rm -f $remote_path"
        else
            # Default: Try interactive first, fall back to non-interactive
            debug "Using default mode (interactive with fallback) for remote execution"
            echo "Attempting interactive execution on remote node $node..."
            if ! ssh -t "root@${node}" "chmod +x $remote_path && $remote_path $action $id $snapshotname $complete_args; rm -f $remote_path" 2>/dev/null; then
                debug "Interactive mode failed, falling back to non-interactive"
                echo "Interactive mode failed, falling back to non-interactive mode..."
                ssh "root@${node}" "chmod +x $remote_path && $remote_path $action $id $snapshotname --non-interactive $complete_args && rm -f $remote_path"
            fi
        fi
    else
        echo "ERROR: Failed to copy script to node $node"
        debug "Failed to copy script to $node"
        return 1
    fi
}


# Execute command on specific node
execute_on_node() {
    local node="$1"
    local command="$2"
    
    debug "Executing command on node $node: $command"
    
    if [ "$node" = "$CURRENT_NODE" ]; then
        debug "Executing locally"
        eval "$command"
    else
        if test_ssh_connection "$node"; then
            debug "Executing via SSH on $node"
            ssh "root@${node}" "$command"
        else
            echo "ERROR: Cannot connect to node $node via SSH"
            echo "HINT: Run './snapshot.sh --setup-ssh' to configure SSH keys"
            debug "SSH connection failed to $node"
            return 1
        fi
    fi
}

# Detect if LV is thin-provisioned
is_thin_lv() {
    local lv_path="$1"
    
    debug "Checking if LV is thin-provisioned: $lv_path"
    
    # Check if LV has thin pool origin (indicating it's thin-provisioned)
    thin_info=$(lvs --noheadings -o pool_lv "$lv_path" 2>/dev/null | tr -d ' ')
    
    if [ -n "$thin_info" ] && [ "$thin_info" != "" ]; then
        debug "LV $lv_path is thin-provisioned (pool: $thin_info)"
        echo "true"
    else
        debug "LV $lv_path is regular LVM"
        echo "false"
    fi
}

# Get thin pool for a thin LV
get_thin_pool() {
    local lv_path="$1"
    
    debug "Getting thin pool for LV: $lv_path"
    
    pool_info=$(lvs --noheadings -o vg_name,pool_lv "$lv_path" 2>/dev/null)
    if [ -n "$pool_info" ]; then
        vg_name=$(echo "$pool_info" | awk '{print $1}')
        pool_name=$(echo "$pool_info" | awk '{print $2}')
        result="${vg_name}/${pool_name}"
        debug "Thin pool for $lv_path: $result"
        echo "$result"
    else
        debug "Could not determine thin pool for $lv_path"
    fi
}

# create a snapshot of a disk (enhanced for both thick and thin LVM)
vm_disk_snapshot_create() {
    local diskpath="$1"
    local snapshotname="$2"
    local snapshotsize="$3"
    
    debug "Creating disk snapshot: $diskpath -> $snapshotname (size: $snapshotsize)"
    
    # parameters check
    if [ -z "$diskpath" ] || [ -z "$snapshotname" ]; then
        echo "Usage: vm_disk_snapshot_create <diskpath> <snapshotname> [<snapshotsize>]"
        return 1
    fi

    # check of diskpath existence
    if [ ! -e "$diskpath" ]; then
        echo "ERROR : disk path '$diskpath' not exists."
        debug "Disk path does not exist: $diskpath"
        return 1
    fi
    
    # Detect if this is a thin LV
    is_thin=$(is_thin_lv "$diskpath")
    snapshot_path="${diskpath}-snapshot-${snapshotname}"
    
    if [ "$DEBUG_MODE" != "true" ]; then
        echo "DEBUG::vm_disk_snapshot_create $1 $2 $3 => ${snapshot_path}"
        echo "Thin LV detected: $is_thin"
    fi
    debug "Snapshot path: $snapshot_path"
    debug "Is thin LV: $is_thin"
    
    if [ "$is_thin" = "true" ]; then
        # Thin LVM snapshot - no size specification needed
        echo "Creating thin LVM snapshot..."
        debug "Using thin LVM snapshot creation"
        
        # Get thin pool info
        thin_pool=$(get_thin_pool "$diskpath")
        if [ -n "$thin_pool" ]; then
            echo "Using thin pool: $thin_pool"
            debug "Thin pool: $thin_pool"
            
            # For thin snapshots, we don't specify size - it's allocated as needed
            debug "Executing: lvcreate -s -n $(basename ${snapshot_path}) $diskpath"
            lvcreate -s -n "$(basename ${snapshot_path})" "$diskpath"
        else
            echo "ERROR: Could not determine thin pool for $diskpath"
            debug "Failed to determine thin pool"
            return 1
        fi
    else
        # Regular LVM snapshot - size specification required
        echo "Creating regular LVM snapshot with size: $snapshotsize"
        debug "Using regular LVM snapshot creation with size: $snapshotsize"
        
        if [ -z "$snapshotsize" ]; then
            echo "ERROR: Snapshot size required for regular LVM"
            debug "No snapshot size provided for regular LVM"
            return 1
        fi
        
        # Check VG space for regular LVM
        vg_name=$(lvs --noheadings -o vg_name "$diskpath" 2>/dev/null | tr -d ' ')
        if [ -n "$vg_name" ]; then
            debug "Volume group: $vg_name"
            free_space=$(vgs --noheadings -o vg_free --units g "$vg_name" 2>/dev/null | tr -d ' ' | sed 's/g//')
            required_gb=$(echo "$snapshotsize" | sed 's/[^0-9]//g')
            
            echo "VG $vg_name free space: ${free_space}G, required: ${required_gb}G"
            debug "VG free space check: ${free_space}G available, ${required_gb}G required"
            
            if [ "$(echo "$free_space < $required_gb" | bc 2>/dev/null || echo "0")" = "1" ]; then
                echo "ERROR: Insufficient space in VG $vg_name. Required: ${snapshotsize}, Available: ${free_space}G"
                debug "Insufficient VG space"
                return 1
            fi
        fi
        
        debug "Executing: lvcreate -L $snapshotsize -s -n $(basename ${snapshot_path}) $diskpath"
        lvcreate -L "$snapshotsize" -s -n "$(basename ${snapshot_path})" "$diskpath"
    fi
    
    # check if command succeeded
    if [ $? -eq 0 ]; then
        echo "Snapshot successfully created : ${snapshot_path}"
        debug "Snapshot creation successful"
        
        # Show snapshot info
        lvs "${snapshot_path}" 2>/dev/null || echo "Note: Snapshot created but may not be immediately visible"
    else
        echo "Snapshot creation failed"
        debug "Snapshot creation failed"
        return 1
    fi
}

# delete snapshot of a disk (works for both thick and thin)
vm_disk_snapshot_delete() {
    local diskpath="$1"
    local snapshotname="$2"
    
    debug "Deleting disk snapshot: $diskpath -> $snapshotname"
    
    # parameters check
    if [ -z "$diskpath" ] || [ -z "$snapshotname" ]; then
        echo "Usage: vm_disk_snapshot_delete <diskpath> <snapshotname>"
        return 1
    fi

    snapshot_path="${diskpath}-snapshot-${snapshotname}"
    
    # Check if snapshot exists
    if ! lvs "${snapshot_path}" >/dev/null 2>&1; then
        echo "ERROR : Snapshot '${snapshot_path}' does not exist."
        debug "Snapshot does not exist: $snapshot_path"
        return 1
    fi
    
    if [ "$DEBUG_MODE" != "true" ]; then
        echo "DEBUG::vm_disk_snapshot_delete $1 $2 => ${snapshot_path}"
    fi
    debug "Deleting snapshot: $snapshot_path"

    # execute lvremove to remove snapshot (works for both thick and thin)
    debug "Executing: lvremove -y $snapshot_path"
    lvremove -y "${snapshot_path}"
    
    # check if command succeeded
    if [ $? -eq 0 ]; then
        echo "Snapshot successfully deleted : ${snapshot_path}"
        debug "Snapshot deletion successful"
    else
        echo "Snapshot delete failed"
        debug "Snapshot deletion failed"
        return 1
    fi
}

# create a copy of a snapshot (enhanced for thin/thick)
vm_disk_snapshot_copy() {
    local diskpath="$1"
    local snapshotname="$2"
    local tempname="$3"
    local snapsize="$4"
    
    debug "Copying disk snapshot: $diskpath -> $snapshotname to $tempname"
    
    # parameters check
    if [ -z "$diskpath" ] || [ -z "$snapshotname" ] || [ -z "$tempname" ]; then
        echo "Usage: vm_disk_snapshot_copy <diskpath> <snapshotname> <tempname> [<snapsize>]"
        return 1
    fi
    
    original_snapshot="${diskpath}-snapshot-${snapshotname}"
    temp_snapshot="${diskpath}-snapshot-${tempname}"
    
    # Check if original snapshot exists
    if ! lvs "${original_snapshot}" >/dev/null 2>&1; then
        echo "ERROR: Original snapshot '${original_snapshot}' does not exist."
        debug "Original snapshot does not exist: $original_snapshot"
        return 1
    fi
    
    echo "Creating backup copy of snapshot: ${temp_snapshot}"
    debug "Creating backup copy: $original_snapshot -> $temp_snapshot"
    
    # Check if original disk is thin
    is_thin=$(is_thin_lv "$diskpath")
    debug "Original disk is thin: $is_thin"
    
    if [ "$is_thin" = "true" ]; then
        # Create thin snapshot
        debug "Creating thin snapshot copy"
        lvcreate -s -n "$(basename ${temp_snapshot})" "$diskpath"
    else
        # Create regular snapshot with specified size
        if [ -z "$snapsize" ]; then
            echo "ERROR: Snapshot size required for regular LVM copy"
            debug "No snapshot size provided for regular LVM copy"
            return 1
        fi
        debug "Creating regular snapshot copy with size: $snapsize"
        lvcreate -L "$snapsize" -s -n "$(basename ${temp_snapshot})" "$diskpath"
    fi
    
    if [ $? -eq 0 ]; then
        echo "Backup snapshot created: ${temp_snapshot}"
        debug "Backup snapshot creation successful"
        return 0
    else
        echo "Failed to create backup snapshot"
        debug "Backup snapshot creation failed"
        return 1
    fi
}

# Enhanced revert disk function - works for both regular and thin LVM
vm_disk_revert_to_snapshot() {
    local diskpath="$1"
    local snapshotname="$2"
    local keep_snapshot="$3"
    
    debug "Reverting disk to snapshot: $diskpath -> $snapshotname (keep: $keep_snapshot)"
    
    # parameters check
    if [ -z "$diskpath" ] || [ -z "$snapshotname" ]; then
        echo "Usage: vm_disk_revert_to_snapshot <diskpath> <snapshotname>"
        return 1
    fi

    # check of diskpath existence
    if [ ! -e "$diskpath" ]; then
        echo "ERROR : disk path '$diskpath' not exists."
        debug "Disk path does not exist: $diskpath"
        return 1
    fi
    
    snapshot_path="${diskpath}-snapshot-${snapshotname}"
    
    # check of snapshot existence
    if ! lvs "${snapshot_path}" >/dev/null 2>&1; then
        echo "ERROR : snapshot '${snapshot_path}' does not exist."
        debug "Snapshot does not exist: $snapshot_path"
        return 1
    fi
    
    # If we want to keep the snapshot, create a backup copy first
    temp_snapshot_name=""
    if [ "$keep_snapshot" = "true" ]; then
        temp_snapshot_name="${snapshotname}-temp-$$"
        debug "Creating backup copy to preserve snapshot: $temp_snapshot_name"
        
        echo "Creating backup copy to preserve original snapshot..."
        vm_disk_snapshot_copy "$diskpath" "$snapshotname" "$temp_snapshot_name"
        
        if [ $? -ne 0 ]; then
            echo "WARNING: Failed to create backup snapshot, original will be lost during revert"
            debug "Backup snapshot creation failed"
            keep_snapshot="false"
        fi
    fi
    
    if [ "$DEBUG_MODE" != "true" ]; then
        echo "DEBUG::vm_disk_revert_to_snapshot $1 $2 => ${snapshot_path}"
    fi
    debug "Starting merge operation for: $snapshot_path"

    # execute lvconvert merge to revert to snapshot
    echo "Starting merge operation for ${snapshot_path}..."
    debug "Executing: lvconvert --merge $snapshot_path"
    lvconvert --merge "${snapshot_path}"
    
    # check if command succeeded
    if [ $? -eq 0 ]; then
        echo "Merge initiated for snapshot: ${snapshot_path}"
        debug "Merge operation started successfully"
        
        # If we're keeping the snapshot, save its info for recreation later
        if [ "$keep_snapshot" = "true" ] && [ -n "$temp_snapshot_name" ]; then
            debug "Saving snapshot info for recreation: $diskpath|$snapshotname|$temp_snapshot_name"
            echo "${diskpath}|${snapshotname}|${temp_snapshot_name}" >> "/tmp/snapshots_to_recreate.$$"
        fi
        
        return 0
    else
        echo "Failed to start merge for snapshot ${snapshot_path}"
        debug "Merge operation failed"
        
        # Clean up temp snapshot if we created one
        if [ "$keep_snapshot" = "true" ] && [ -n "$temp_snapshot_name" ]; then
            debug "Cleaning up temp snapshot: ${diskpath}-snapshot-${temp_snapshot_name}"
            lvremove -y "${diskpath}-snapshot-${temp_snapshot_name}" 2>/dev/null
        fi
        
        return 1
    fi
}

# Recreate snapshots after merge completes (enhanced)
recreate_snapshots() {
    tmp_file="/tmp/snapshots_to_recreate.$$"
    
    debug "Checking for snapshots to recreate: $tmp_file"
    
    if [ ! -f "$tmp_file" ]; then
        debug "No snapshots to recreate"
        return 0
    fi
    
    echo "Recreating preserved snapshots..."
    debug "Processing snapshots recreation file"
    
    # Process each line in the temp file
    while IFS='|' read -r diskpath snapshotname tempname; do
        debug "Processing snapshot recreation: $diskpath -> $snapshotname (from $tempname)"
        
        # Wait for the temporary snapshot to be fully available
        sleep 2
        
        echo "Recreating original snapshot '${snapshotname}' from temp copy"
        
        # Check if original disk is thin
        is_thin=$(is_thin_lv "$diskpath")
        debug "Disk is thin: $is_thin"
        
        if [ "$is_thin" = "true" ]; then
            # Create new thin snapshot
            debug "Creating new thin snapshot: ${diskpath##*/}-snapshot-${snapshotname}"
            lvcreate -s -n "${diskpath##*/}-snapshot-${snapshotname}" "$diskpath"
        else
            # Get size of temp snapshot and create regular snapshot
            snap_info=$(lvs --noheadings -o lv_size --units m "${diskpath}-snapshot-${tempname}" 2>/dev/null)
            if [ -z "$snap_info" ]; then
                echo "WARNING: Could not determine temp snapshot size, using default"
                debug "Could not determine temp snapshot size"
                snap_info="1000M"
            fi
            snap_size=$(echo "$snap_info" | tr -d ' ')
            debug "Creating regular snapshot with size: $snap_size"
            
            lvcreate -L "${snap_size}" -s -n "${diskpath##*/}-snapshot-${snapshotname}" "$diskpath"
        fi
        
        if [ $? -eq 0 ]; then
            echo "Successfully recreated snapshot: ${diskpath}-snapshot-${snapshotname}"
            debug "Snapshot recreation successful"
        else
            echo "Failed to recreate snapshot: ${diskpath}-snapshot-${snapshotname}"
            debug "Snapshot recreation failed"
        fi
        
        # Remove the temporary snapshot
        debug "Removing temporary snapshot: ${diskpath}-snapshot-${tempname}"
        lvremove -y "${diskpath}-snapshot-${tempname}" 2>/dev/null
    done < "$tmp_file"
    
    # Clean up temp file
    debug "Cleaning up snapshots recreation file: $tmp_file"
    rm -f "$tmp_file"
}

# Improved check snapshot status function - works for both regular and thin LVM
check_snapshot_status() {
    local diskpath="$1"
    
    debug "Checking snapshot status for: $diskpath"
    
    # Check if there are any merging snapshots using multiple methods
    
    # Method 1: Check for merging state in lvs output
    if lvs --noheadings -o lv_name,origin "$diskpath" 2>/dev/null | grep -q "merging"; then
        debug "Found merging snapshot (method 1)"
        return 0  # Still merging
    fi
    
    # Method 2: Check for snapshot status in lvdisplay (for regular LVM)
    if lvdisplay "$diskpath" 2>/dev/null | grep -q "LV snapshot status.*active destination for"; then
        debug "Found merging snapshot (method 2)"
        return 0  # Still merging
    fi
    
    # Method 3: Check if any snapshot volumes are still present for this disk
    disk_basename=$(basename "$diskpath")
    if lvs 2>/dev/null | grep -q "${disk_basename}-snapshot-.*merging"; then
        debug "Found merging snapshot (method 3)"
        return 0  # Still merging
    fi
    
    debug "No merging snapshots found"
    return 1  # No longer merging
}

# list instance disks, except local: type (enhanced for containers)
instance_list_disks() {
    local id="$1"
    local is_container="$2"
    
    debug "Listing disks for instance: $id (container: $is_container)"
    
    # parameters check
    if [ -z "$id" ]; then
        echo "Usage: instance_list_disks <id> <is_container>"
        return 1
    fi

    # Get config based on instance type
    config_output=$(get_config "$id" "$is_container")
    debug "Retrieved config for instance $id"
    
    disks=""
    if [ "$is_container" = "true" ]; then
        # For containers, look for rootfs and mp (mount points)
        disks=$(echo "$config_output" | grep -E '^rootfs:|^mp[0-9]+:' | grep -v 'local:')
        debug "Container disks found: $(echo "$disks" | wc -l) lines"
    else
        # For VMs, look for virtio, sata, scsi, ide
        disks=$(echo "$config_output" | grep -E '^virtio[0-9]+:|^sata[0-9]+:|^scsi[0-9]+:|^ide[0-9]+:' | grep -v 'local:')
        debug "VM disks found: $(echo "$disks" | wc -l) lines"
    fi

    # parse each line containing disk and add lvm path
    echo "$disks" | while IFS= read -r line; do
        if [ -z "$line" ]; then
            continue
        fi
        
        debug "Processing disk line: $line"
        
        # extract disk name (field after ':')
        disk_name=""
        if [ "$is_container" = "true" ]; then
            # For containers: rootfs: pve:vm-101-disk-0,size=8G
            # or mp0: pve:vm-101-disk-1,mp=/data,size=10G
            disk_name=$(echo "$line" | cut -d ' ' -f 2 | cut -d ',' -f 1)
        else
            # For VMs: virtio0: pve:vm-104-disk-1,size=32G
            disk_name=$(echo "$line" | cut -d ' ' -f 2 | cut -d ',' -f 1)
        fi
        
        debug "Disk name: $disk_name"
        
        # extract part after ':' in disk_name
        disk_basename=$(echo "$disk_name" | cut -d ':' -f 2)    
        debug "Disk basename: $disk_basename"
        
        # get source path (real target) of symbolic link
        lvm_path=$(find /dev -name "$disk_basename" -type l -exec ls -f {} + 2>/dev/null)
        debug "LVM path: $lvm_path"

        if [ -n "$lvm_path" ]; then
            echo "$line,$disk_basename,$lvm_path"
        else
            echo "$line,$disk_basename,"
        fi
    done
}

# create snapshot for each instance disk with enhanced thin/thick support
instance_snapshot_create() {
    local id="$1"
    local snapshotname="$2"
    local is_container="$3"
    
    debug "Creating instance snapshot: $id -> $snapshotname (container: $is_container)"
    
    # parameters check
    if [ -z "$id" ] || [ -z "$snapshotname" ]; then
        echo "ERROR: Both ID and snapshotname must be provided."
        return 1
    fi
    
    instance_type="VM"
    if [ "$is_container" = "true" ]; then
        instance_type="Container"
    fi
    
    echo "Creating snapshot '$snapshotname' for $instance_type $id..."
    
    # get list of instance disks
    disks=$(instance_list_disks "$id" "$is_container")
    debug "Retrieved disk list for instance $id"
    
    # Check if we found any disks
    if [ -z "$disks" ]; then
        echo "ERROR: No disks found for $instance_type $id"
        debug "No disks found for instance $id"
        return 1
    fi
    
    # Parse each disk line
    echo "$disks" | while IFS= read -r line; do
        if [ -z "$line" ]; then
            continue
        fi
        
        debug "Processing disk line: $line"
        
        # Extract device path (last element after comma)
        device_path=$(echo "$line" | awk -F ',' '{print $NF}')
        debug "Device path: $device_path"
        
        # Skip if no valid device path
        if [ -z "$device_path" ] || [ ! -e "$device_path" ]; then
            echo "WARNING: Invalid device path found in disk list: $device_path"
            debug "Invalid device path: $device_path"
            continue
        fi
        
        echo "Processing disk: $device_path"
        
        # Check if this is a thin LV
        is_thin=$(is_thin_lv "$device_path")
        echo "Thin LV: $is_thin"
        
        if [ "$is_thin" = "true" ]; then
            # For thin LVM, we don't need to specify size
            echo "Creating thin LVM snapshot for: $device_path"
            debug "Creating thin snapshot for: $device_path"
            vm_disk_snapshot_create "$device_path" "$snapshotname"
        else
            # For regular LVM, calculate intelligent snapshot size
            debug "Calculating snapshot size for regular LVM"
            snapshot_size=""
            
            # Extract disk size from config
            config_size=$(echo "$line" | grep -oP 'size=\K[^,]+' || echo "")
            debug "Config size: $config_size"
            
            if [ -n "$config_size" ]; then
                # Calculate percentage based on disk size
                size_num=$(echo "$config_size" | grep -o '[0-9]\+')
                debug "Size number: $size_num"
                
                if [ "$size_num" -lt 50 ]; then
                    # Small disk (< 50G): 25%
                    snapshot_size=$(echo "$size_num * 0.25" | bc 2>/dev/null | awk '{printf "%.0f", $1}' || echo "10")
                elif [ "$size_num" -lt 200 ]; then
                    # Medium disk (50-200G): 20%  
                    snapshot_size=$(echo "$size_num * 0.20" | bc 2>/dev/null | awk '{printf "%.0f", $1}' || echo "15")
                else
                    # Large disk (> 200G): 15%
                    snapshot_size=$(echo "$size_num * 0.15" | bc 2>/dev/null | awk '{printf "%.0f", $1}' || echo "20")
                fi
                
                # Add unit
                size_unit=$(echo "$config_size" | grep -o '[A-Za-z]\+' || echo "G")
                if [ -z "$size_unit" ]; then
                    size_unit="G"
                fi
                snapshot_size="${snapshot_size}${size_unit}"
            else
                # Fallback: use 15% of actual disk size
                debug "Using fallback size calculation"
                disk_size=$(lvs "$device_path" --noheadings -o lv_size --units g 2>/dev/null | tr -d ' ')
                size_in_gb=$(echo "$disk_size" | sed 's/g//')
                snapshot_size=$(echo "$size_in_gb * 0.15" | bc 2>/dev/null | awk '{printf "%.0f", $1}' || echo "10")
                snapshot_size="${snapshot_size}G"
            fi
            
            debug "Calculated snapshot size: $snapshot_size"
            echo "Creating regular LVM snapshot for: $device_path (Size: $snapshot_size)"
            vm_disk_snapshot_create "$device_path" "$snapshotname" "$snapshot_size"
        fi
    done
    
    echo -e "\nSnapshot '$snapshotname' creation process completed for $instance_type $id"
    echo "Use './snapshot.sh list $id' to see created snapshots"
    debug "Instance snapshot creation completed"
    
    return 0
}

# snapshot delete for each instance disk
instance_snapshot_delete() {
    local id="$1"
    local snapshotname="$2"
    local is_container="$3"
    
    debug "Deleting instance snapshot: $id -> $snapshotname (container: $is_container)"
    
    # parameters check
    if [ -z "$id" ] || [ -z "$snapshotname" ]; then
        echo "ERROR : Both ID and snapshotname must be provided."
        return 1
    fi
    
    # get list of instance disks
    disks=$(instance_list_disks "$id" "$is_container")
    debug "Retrieved disk list for deletion"
    
    # parse each disk line
    echo "$disks" | while IFS= read -r line; do
        if [ -z "$line" ]; then
            continue
        fi
        
        # extract device path (last element after comma)
        device_path=$(echo "$line" | awk -F ',' '{print $NF}')
        debug "Deleting snapshot for device: $device_path"
        
        # call function to delete disk snapshot
        vm_disk_snapshot_delete "$device_path" "$snapshotname"
    done
    
    debug "Instance snapshot deletion completed"
    return 0
}

# Fixed revert function with improved interactive/non-interactive handling for instances
instance_revert_to_snapshot() {
    local id="$1"
    local snapshotname="$2"
    local autostart="$3"
    local keep_snapshot="$4"
    local is_container="$5"

    debug "Reverting instance to snapshot: $id -> $snapshotname (autostart: $autostart, keep: $keep_snapshot, container: $is_container)"

    # parameters check
    if [ -z "$id" ] || [ -z "$snapshotname" ]; then
        echo "Usage: instance_revert_to_snapshot <id> <snapshotname>"
        return 1
    fi

    instance_type="VM"
    if [ "$is_container" = "true" ]; then
        instance_type="Container"
    fi

    # check if instance exists
    if ! get_status "$id" "$is_container" >/dev/null 2>&1; then
        echo "ERROR: $instance_type $id does not exist"
        debug "$instance_type $id does not exist"
        return 1
    fi

    # check if instance is running and store initial state
    instance_status=$(get_status "$id" "$is_container")
    instance_was_running=false
    
    debug "Instance status: $instance_status"
    
    if echo "$instance_status" | grep -q "status: running"; then
        instance_was_running=true
        debug "Instance was running"
    else
        debug "Instance was not running"
    fi

    # Handle snapshot preservation based on mode
    if [ "$NON_INTERACTIVE" = "true" ] && [ "$FORCE_INTERACTIVE" != "true" ]; then
        # Non-interactive mode: use provided parameter or default
        if [ -z "$keep_snapshot" ]; then
            keep_snapshot="false"  # Default for non-interactive
        fi
        echo "Non-interactive mode: keep_snapshot=$keep_snapshot"
        debug "Non-interactive mode, keep_snapshot=$keep_snapshot"
    else
        # Interactive mode: ask user if not explicitly set
        if [ -z "$keep_snapshot" ]; then
            read -p "Do you want to keep the snapshot after reverting? (y/n): " answer
            if [ "$answer" = "y" ] || [ "$answer" = "Y" ]; then
                keep_snapshot="true"
                echo "Snapshot will be preserved after reverting."
                debug "User chose to keep snapshot"
            else
                keep_snapshot="false"
                debug "User chose not to keep snapshot"
            fi
        else
            debug "Keep snapshot explicitly set to: $keep_snapshot"
        fi
    fi

    # ask confirmation to shutdown instance if running
    if echo "$instance_status" | grep -q "status: running"; then
        if [ "$NON_INTERACTIVE" = "true" ] && [ "$FORCE_INTERACTIVE" != "true" ]; then
            echo "Non-interactive mode: Proceeding with $instance_type shutdown for revert..."
            debug "Non-interactive: proceeding with shutdown"
        else
            read -p "$instance_type $id will be stopped during the revert process. Continue? (y/n): " answer
            if [ "$answer" != "y" ]; then
                echo "Task canceled."
                debug "User canceled revert operation"
                return 1
            fi
            debug "User confirmed instance shutdown"
        fi
        
        debug "Stopping $instance_type $id"
        stop_instance "$id" "$is_container"
        
        # Wait for instance to stop
        wait_count=0
        while [ "$wait_count" -lt 30 ]; do
            if ! get_status "$id" "$is_container" | grep -q "status: running"; then
                break
            fi
            echo -n "."
            sleep 2
            wait_count=$((wait_count + 1))
            debug "Waiting for instance to stop: $wait_count/30"
        done
        
        if get_status "$id" "$is_container" | grep -q "status: running"; then
            echo "WARNING: $instance_type did not stop gracefully, forcing stop..."
            debug "Forcing instance stop"
            if [ "$is_container" = "true" ]; then
                pct stop "$id" --skiplock 2>/dev/null || pct stop "$id" --force
            else
                qm stop "$id" --skiplock
            fi
            sleep 5
        fi
        echo "$instance_type stopped successfully."
        debug "Instance stopped successfully"
    fi
        
    # get instance disk list
    disks=$(instance_list_disks "$id" "$is_container")
    debug "Retrieved disk list for revert"
    
    if [ "$DEBUG_MODE" != "true" ]; then
        echo "DEBUG::instance_revert_to_snapshot $1 $2"
    fi
    
    # Create temp file for snapshots to recreate
    rm -f "/tmp/snapshots_to_recreate.$$" 2>/dev/null
    debug "Cleaned up temp files"
    
    # parse each disk line for reverting
    revert_success=true
    echo "$disks" | while IFS= read -r line; do
        if [ -z "$line" ]; then
            continue
        fi
        
        # extract device path (last element after comma)
        device_path=$(echo "$line" | awk -F ',' '{print $NF}')
        
        # check if device path is valid
        if [ -z "$device_path" ] || [ ! -e "$device_path" ]; then
            echo "WARNING: Invalid device path: $device_path"
            debug "Invalid device path: $device_path"
            continue
        fi
        
        echo "Reverting disk: $device_path"
        debug "Reverting disk: $device_path"
        
        # call function to revert disk to snapshot
        if ! vm_disk_revert_to_snapshot "$device_path" "$snapshotname" "$keep_snapshot"; then
            echo "ERROR: Failed to revert disk $device_path"
            debug "Failed to revert disk: $device_path"
            revert_success=false
        fi
    done

    # Wait a bit for all merges to start
    echo "Waiting for snapshot merge operations to complete..."
    debug "Starting merge completion wait"
    sleep 5
    
    # Check if any snapshots are still merging with improved detection
    still_merging=true
    max_wait=300  # 5 minutes max wait
    wait_time=0
    
    while [ "$still_merging" = "true" ] && [ "$wait_time" -lt "$max_wait" ]; do
        still_merging=false
        debug "Checking for ongoing merges (wait time: $wait_time/$max_wait)"
        
        # Check each disk for ongoing merges
        echo "$disks" | while IFS= read -r line; do
            if [ -z "$line" ]; then
                continue
            fi
            
            device_path=$(echo "$line" | awk -F ',' '{print $NF}')
            
            if [ -n "$device_path" ] && [ -e "$device_path" ]; then
                # Check if there's an active merge using multiple methods
                if check_snapshot_status "$device_path"; then
                    debug "Still merging: $device_path"
                    still_merging=true
                    break
                fi
            fi
        done
        
        if [ "$still_merging" = "true" ]; then
            echo -n "."
            sleep 10
            wait_time=$((wait_time + 10))
        fi
    done
    
    if [ "$wait_time" -ge "$max_wait" ]; then
        echo ""
        echo "WARNING: Merge operation took longer than expected. Proceeding anyway..."
        debug "Merge operation timed out"
    else
        echo ""
        echo "Snapshot merge operations completed."
        debug "All merge operations completed"
    fi
    
    # Recreate snapshots if requested
    if [ "$keep_snapshot" = "true" ]; then
        debug "Recreating snapshots"
        recreate_snapshots
    fi

    # Decide whether to restart the instance
    if [ "$autostart" = "true" ] || [ "$instance_was_running" = "true" ]; then
        debug "Starting $instance_type $id"
        start_instance "$id" "$is_container"
        
        # Wait for instance to start
        wait_count=0
        while [ "$wait_count" -lt 30 ]; do
            if get_status "$id" "$is_container" | grep -q "status: running"; then
                echo "$instance_type $id started successfully."
                debug "Instance started successfully"
                break
            fi
            echo -n "."
            sleep 2
            wait_count=$((wait_count + 1))
            debug "Waiting for instance to start: $wait_count/30"
        done
        
        if ! get_status "$id" "$is_container" | grep -q "status: running"; then
            echo "WARNING: $instance_type $id did not start automatically. You may need to start it manually."
            debug "Instance failed to start automatically"
        fi
    else
        echo "$instance_type $id remains stopped. Use 'pct start $id' or 'qm start $id' to start it when ready."
        debug "Instance left in stopped state"
    fi

    debug "Instance revert operation completed"
    return 0
}

# Snapshot list function for instances
instance_snapshot_list() {
    local id="$1"
    local is_container="$2"
    
    debug "Listing snapshots for instance: $id (container: $is_container)"
    
    # parameters check
    if [ -z "$id" ]; then
        echo "Usage: instance_snapshot_list <id> <is_container>"
        return 1
    fi
    
    instance_type="VM"
    if [ "$is_container" = "true" ]; then
        instance_type="Container"
    fi
    
    echo "========================= Snapshots for $instance_type $id ========================="
    echo "| Snapshot Name | Disk        | Size    | Usage | Type | Creation Date        |"
    echo "|---------------|-------------|---------|-------|------|----------------------|"
    
    # Find all volume groups
    for vg in $(vgs --noheadings -o vg_name 2>/dev/null | tr -d ' '); do
        debug "Checking volume group: $vg"
        
        # Find instance's disk snapshots
        lvs "$vg" --separator ":" --noheadings -o lv_name,lv_size,data_percent,origin,pool_lv,lv_time 2>/dev/null | grep -E "vm-${id}-disk|vm--${id}--disk" | grep "snapshot" | while IFS=":" read -r lv_name lv_size usage origin pool_lv creation_time_raw; do
            debug "Processing snapshot: $lv_name"
            
            # Clean up whitespace
            lv_name=$(echo "$lv_name" | tr -d ' ')
            lv_size=$(echo "$lv_size" | tr -d ' ')
            usage=$(echo "$usage" | tr -d ' ')
            pool_lv=$(echo "$pool_lv" | tr -d ' ')
            creation_time_raw=$(echo "$creation_time_raw" | tr -d ' ')
            
            # Extract snapshot name
            snapshot_name=$(echo "$lv_name" | sed -n 's/.*snapshot-\(.*\)/\1/p')
            debug "Snapshot name: $snapshot_name"
            
            # Extract disk info
            disk_info="unknown"
            if echo "$lv_name" | grep -q "disk-"; then
                disk_info=$(echo "$lv_name" | grep -oE 'disk-[0-9]+|disk--[0-9]+' | sed -e 's/disk-/disk-/' -e 's/disk--/disk-/')
            elif echo "$lv_name" | grep -q "rootfs"; then
                disk_info="rootfs"
            fi
            debug "Disk info: $disk_info"
            
            # Determine type
            snap_type="Thick"
            if [ -n "$pool_lv" ]; then
                snap_type="Thin"
            fi
            debug "Snapshot type: $snap_type"
            
            # Simple date formatting
            creation_date="Unknown"
            if [ -n "$creation_time_raw" ] && echo "$creation_time_raw" | grep -qE '^[0-9]+$'; then
                creation_date=$(date -d "@$creation_time_raw" "+%Y-%m-%d %H:%M" 2>/dev/null || echo "Unknown")
            fi
            debug "Formatted creation date: $creation_date"
            
            # Format usage percentage
            if [ -n "$usage" ] && [ "$usage" != "100.00" ]; then
                usage="${usage}%"
            else
                usage="N/A"
            fi
            
            # Print snapshot info in table format
            printf "| %-13s | %-10s | %-7s | %-5s | %-4s | %-20s |\n" "$snapshot_name" "$disk_info" "$lv_size" "$usage" "$snap_type" "$creation_date"
        done
    done
    
    echo "============================================================================"
    
    # Check if we found any snapshots
    if ! lvs --noheadings 2>/dev/null | grep -E "vm-${id}-disk|vm--${id}--disk" | grep -q "snapshot"; then
        echo "No snapshots found for $instance_type $id"
        debug "No snapshots found for instance $id"
    fi
}

# Enhanced main detection with better error handling
enhanced_detect_and_proceed() {
    local id="$1"
    local force_vm="$2"
    local force_container="$3"
    
    is_container="false"
    
    if [ "$force_container" = "true" ]; then
        is_container="true"
        debug "Forced container mode"
        echo "Forced container mode for ID $id"
        return 0
    elif [ "$force_vm" = "true" ]; then
        is_container="false"
        debug "Forced VM mode"
        echo "Forced VM mode for ID $id"
        return 0
    else
        # Enhanced auto-detection
        echo "Auto-detecting type for ID $id..."
        
        detect_container_or_vm "$id"
        detection_result=$?
        
        case $detection_result in
            0)
                is_container="true"
                debug "Auto-detected as container"
                echo "✓ Auto-detected as Container"
                return 0
                ;;
            1)
                is_container="false"
                debug "Auto-detected as VM"
                echo "✓ Auto-detected as VM"
                return 0
                ;;
            2)
                echo ""
                echo "❌ Could not automatically determine type for ID $id"
                echo ""
                show_detection_details "$id"
                echo ""
                echo "Please specify the type manually:"
                echo "  For containers: $0 $* --container"
                echo "  For VMs:        $0 $* --vm"
                echo ""
                echo "Or check if the ID exists:"
                if [ "$CLUSTER_MODE" = "true" ]; then
                    echo "  pct list    # List all containers"
                    echo "  qm list     # List all VMs"
                    echo "  pvesh get /cluster/resources  # Show all cluster resources"
                else
                    echo "  pct list    # List all containers"
                    echo "  qm list     # List all VMs"
                fi
                return 1
                ;;
        esac
    fi
}

# Clean up function
cleanup() {
    debug "Cleaning up temporary files"
    rm -f "/tmp/snapshots_to_recreate.$$" 2>/dev/null
}

# Set up trap to clean up on exit
trap cleanup EXIT INT TERM

# Show support message at start
show_support_message

# Initialize cluster support
init_cluster_support

# Check for special commands
if [ "$1" = "--setup-ssh" ]; then
    setup_ssh_keys
    exit 0
fi

# parameters get
action="$1"
id="$2"
snapshotname="$3"
autostart="true"  # Default: start instance after revert
force_local="false"
cluster_sync="false"
keep_snapshot=""
force_vm="false"
force_container="false"

# Check for options (add the --no-banner option)
for arg in "$@"; do
    case "$arg" in
        --no-autostart)
            autostart="false"
            debug "Option: no-autostart"
            ;;
        --force-local)
            force_local="true"
            debug "Option: force-local"
            ;;
        --cluster-sync)
            cluster_sync="true"
            debug "Option: cluster-sync"
            ;;
        --non-interactive)
            NON_INTERACTIVE="true"
            debug "Option: non-interactive"
            ;;
        --interactive)
            FORCE_INTERACTIVE="true"
            debug "Option: force-interactive"
            ;;
        --keep-snapshot)
            keep_snapshot="true"
            debug "Option: keep-snapshot"
            ;;
        --delete-snapshot)
            keep_snapshot="false"
            debug "Option: delete-snapshot"
            ;;
        --debug)
            DEBUG_MODE="true"
            debug "Debug mode enabled"
            ;;
        --container)
            force_container="true"
            CONTAINER_MODE="true"
            debug "Option: force-container"
            ;;
        --vm)
            force_vm="true"
            debug "Option: force-vm"
            ;;
        --no-banner)
            SHOW_BANNER="false"
            debug "Option: no-banner (suppressing support message)"
            ;;
    esac
done

# parameters check
if [ -z "$action" ] || [ -z "$id" ]; then
    echo "ERROR : action and ID must be specified."
    usage
    exit 1
fi

# Enhanced detection with better error handling
if ! enhanced_detect_and_proceed "$id" "$force_vm" "$force_container"; then
    exit 1
fi

debug "Action: $action, ID: $id, Snapshot: $snapshotname, Container: $is_container"
debug "Options: autostart=$autostart, force_local=$force_local, keep_snapshot=$keep_snapshot"

# Show available instances if not found
show_available_instances() {
    echo ""
    echo "Available VMs and Containers on this cluster:"
    debug "Showing available instances"
    if [ "$CLUSTER_MODE" = "true" ]; then
        for node in $CLUSTER_NODES; do
            echo "--- Node: $node ---"
            echo "VMs:"
            execute_on_node "$node" "qm list" 2>/dev/null || echo "Cannot access VMs on node $node"
            echo "Containers:"
            execute_on_node "$node" "pct list" 2>/dev/null || echo "Cannot access containers on node $node"
        done
    else
        echo "VMs:"
        qm list
        echo "Containers:"
        pct list
    fi
}

# Switch parameter with cluster awareness
case "$action" in
    create)
        if [ -z "$snapshotname" ]; then
            echo "ERROR : snapshotname parameter required for create action."
            usage
            exit 1
        fi
        
        debug "Starting create action"
        
        # Check if force-local is requested
        if [ "$force_local" = "true" ]; then
            debug "Force local execution"
            instance_snapshot_create "$id" "$snapshotname" "$is_container"
        else
            # Try to find and execute on correct node
            instance_node=$(get_instance_node "$id" "$is_container")
            if [ -z "$instance_node" ]; then
                echo "ERROR: Instance $id not found in cluster"
                debug "Instance $id not found in cluster"
                show_available_instances
                exit 1
            fi
            
            if [ "$instance_node" = "$CURRENT_NODE" ]; then
                instance_type="VM"
                if [ "$is_container" = "true" ]; then
                    instance_type="Container"
                fi
                echo "$instance_type $id found locally - executing snapshot creation"
                debug "Executing locally on $CURRENT_NODE"
                instance_snapshot_create "$id" "$snapshotname" "$is_container"
            else
                instance_type="VM"
                if [ "$is_container" = "true" ]; then
                    instance_type="Container"
                fi
                echo "$instance_type $id found on node: $instance_node - executing remotely"
                debug "Executing remotely on $instance_node"
                execute_remote_snapshot "$instance_node" "create" "$id" "$snapshotname"
            fi
        fi
        ;;
    delete)
        if [ -z "$snapshotname" ]; then
            echo "ERROR : snapshotname parameter required for delete action."
            usage
            exit 1
        fi
        
        debug "Starting delete action"
        
        if [ "$force_local" = "true" ]; then
            debug "Force local execution"
            instance_snapshot_delete "$id" "$snapshotname" "$is_container"
        else
            instance_node=$(get_instance_node "$id" "$is_container")
            if [ -z "$instance_node" ]; then
                echo "ERROR: Instance $id not found in cluster"
                debug "Instance $id not found in cluster"
                show_available_instances
                exit 1
            fi
            
            if [ "$instance_node" = "$CURRENT_NODE" ]; then
                instance_type="VM"
                if [ "$is_container" = "true" ]; then
                    instance_type="Container"
                fi
                echo "$instance_type $id found locally - executing snapshot deletion"
                debug "Executing locally on $CURRENT_NODE"
                instance_snapshot_delete "$id" "$snapshotname" "$is_container"
            else
                instance_type="VM"
                if [ "$is_container" = "true" ]; then
                    instance_type="Container"
                fi
                echo "$instance_type $id found on node: $instance_node - executing remotely"
                debug "Executing remotely on $instance_node"
                execute_remote_snapshot "$instance_node" "delete" "$id" "$snapshotname"
            fi
        fi
        ;;
    revert)
        if [ -z "$snapshotname" ]; then
            echo "ERROR : snapshotname parameter required for revert action"
            usage
            exit 1
        fi
        
        debug "Starting revert action"
        
        extra_args=""
        if [ "$autostart" = "false" ]; then
            extra_args="$extra_args --no-autostart"
        fi
        if [ "$keep_snapshot" = "true" ]; then
            extra_args="$extra_args --keep-snapshot"
        elif [ "$keep_snapshot" = "false" ]; then
            extra_args="$extra_args --delete-snapshot"
        fi
        
        debug "Extra args: $extra_args"
        
        if [ "$force_local" = "true" ]; then
            debug "Force local execution"
            instance_revert_to_snapshot "$id" "$snapshotname" "$autostart" "$keep_snapshot" "$is_container"
        else
            instance_node=$(get_instance_node "$id" "$is_container")
            if [ -z "$instance_node" ]; then
                echo "ERROR: Instance $id not found in cluster"
                debug "Instance $id not found in cluster"
                show_available_instances
                exit 1
            fi
            
            if [ "$instance_node" = "$CURRENT_NODE" ]; then
                instance_type="VM"
                if [ "$is_container" = "true" ]; then
                    instance_type="Container"
                fi
                echo "$instance_type $id found locally - executing snapshot revert"
                debug "Executing locally on $CURRENT_NODE"
                instance_revert_to_snapshot "$id" "$snapshotname" "$autostart" "$keep_snapshot" "$is_container"
            else
                instance_type="VM"
                if [ "$is_container" = "true" ]; then
                    instance_type="Container"
                fi
                echo "$instance_type $id found on node: $instance_node - executing remotely"
                debug "Executing remotely on $instance_node"
                execute_remote_snapshot "$instance_node" "revert" "$id" "$snapshotname" "$extra_args"
            fi
        fi
        ;;
    list)
        debug "Starting list action"
        
        if [ "$force_local" = "true" ]; then
            debug "Force local execution"
            instance_snapshot_list "$id" "$is_container"
        else
            instance_node=$(get_instance_node "$id" "$is_container")
            if [ -z "$instance_node" ]; then
                echo "ERROR: Instance $id not found in cluster"
                debug "Instance $id not found in cluster"
                show_available_instances
                exit 1
            fi
            
            if [ "$instance_node" = "$CURRENT_NODE" ]; then
                instance_type="VM"
                if [ "$is_container" = "true" ]; then
                    instance_type="Container"
                fi
                echo "$instance_type $id found locally - showing snapshots"
                debug "Executing locally on $CURRENT_NODE"
                instance_snapshot_list "$id" "$is_container"
            else
                instance_type="VM"
                if [ "$is_container" = "true" ]; then
                    instance_type="Container"
                fi
                echo "$instance_type $id found on node: $instance_node - showing snapshots remotely"
                debug "Executing remotely on $instance_node"
                execute_remote_snapshot "$instance_node" "list" "$id" ""
            fi
        fi
        ;;
    *)
        echo "Action unknown : $action"
        debug "Unknown action: $action"
        usage
        exit 1
        ;;
esac

debug "Script execution completed"
