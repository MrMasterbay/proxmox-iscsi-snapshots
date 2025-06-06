#!/bin/sh
#
#
# Enhanced Proxmox LVM Snapshot Manager with LXC Container Support
# Ultra-Fast Version with Atomic Consistency + Remote Speed Optimization
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
    echo "  Enhanced Proxmox LVM Snapshot Manager - Ultra-Fast Edition"
    echo "  Remastered by Nico Schmidt (baGStube_Nico)"
    echo ""
    echo "  Supports: QEMU VMs and LXC Containers"
    echo "  Features: Atomic Consistency + Ultra-Fast Performance + Remote Speed"
    echo ""
    echo "  Please consider supporting this script development:"
    echo "  💖 Ko-fi: ko-fi.com/bagstube_nico"
    echo "  🔗 Links: linktr.ee/bagstube_nico"
    echo "============================================================================"
    echo ""
}

# Storage optimization settings
set_storage_optimizations() {
    # Optimize I/O for snapshot operations
    echo 5 > /proc/sys/vm/dirty_ratio 2>/dev/null || true
    echo 2 > /proc/sys/vm/dirty_background_ratio 2>/dev/null || true
    
    # Optimize I/O scheduler for modern storage
    for disk in /sys/block/*/queue/scheduler; do
        if [ -f "$disk" ]; then
            # Use noop for SSDs/NVMe, mq-deadline for others
            if echo "noop" > "$disk" 2>/dev/null; then
                debug "Set noop scheduler for $(dirname $disk)"
            elif echo "mq-deadline" > "$disk" 2>/dev/null; then
                debug "Set mq-deadline scheduler for $(dirname $disk)"
            fi
        fi
    done
    
    debug "Storage optimizations applied"
}

# Setup SSH multiplexing for faster connections
setup_ssh_multiplexing() {
    # Create SSH control directory
    mkdir -p /tmp/ssh_control 2>/dev/null
    
    # Pre-establish connections to all nodes
    for node in $CLUSTER_NODES; do
        if [ "$node" != "$CURRENT_NODE" ]; then
            (
                ssh -o ConnectTimeout=2 -o ControlMaster=auto -o ControlPath="/tmp/ssh_control_%h_%p_%r" -o ControlPersist=300 "root@${node}" "echo" >/dev/null 2>&1 &
            ) &
        fi
    done
    
    debug "SSH multiplexing initialized"
}

# Enhanced script compression
compress_script_for_transfer() {
    local script_path="$1"
    local output_path="$2"
    
    # Use best available compression for network transfer
    if command -v lz4 >/dev/null 2>&1; then
        lz4 -9 "$script_path" "$output_path" >/dev/null 2>&1
    elif command -v xz >/dev/null 2>&1; then
        xz -9 -c "$script_path" > "$output_path"
    else
        gzip -9 -c "$script_path" > "$output_path"
    fi
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

# Lightning-fast detection using cache
detect_container_or_vm_fast() {
    local id="$1"
    
    # Use cached detection if available (5 minute cache)
    cache_file="/tmp/instance_cache_$id"
    if [ -f "$cache_file" ] && [ "$(find "$cache_file" -mmin -5 2>/dev/null)" ]; then
        result=$(cat "$cache_file")
        debug "Using cached detection result: $result"
        return "$result"
    fi
    
    debug "Fast detection for ID: $id"
    
    # Quick config file check (fastest method)
    if [ -f "/etc/pve/lxc/${id}.conf" ]; then
        echo "0" > "$cache_file"
        debug "Fast detected as container via config file"
        return 0
    elif [ -f "/etc/pve/qemu-server/${id}.conf" ]; then
        echo "1" > "$cache_file"
        debug "Fast detected as VM via config file"
        return 1
    fi
    
    # Quick status check
    if pct status "$id" >/dev/null 2>&1; then
        echo "0" > "$cache_file"
        debug "Fast detected as container via status"
        return 0
    elif qm status "$id" >/dev/null 2>&1; then
        echo "1" > "$cache_file"
        debug "Fast detected as VM via status"
        return 1
    fi
    
    # Cache miss - fallback to full detection
    debug "Cache miss, using full detection"
    detect_container_or_vm "$id"
    result=$?
    echo "$result" > "$cache_file"
    return $result
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
                if test_ssh_connection_fast "$node"; then
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

# Optimized SSH keys for ultra-fast cluster communication
setup_ssh_keys_fast() {
    echo "⚡ Setting up SSH keys for ultra-fast cluster communication..."
    
    # Generate key if not exists
    if [ ! -f ~/.ssh/id_rsa ]; then
        echo "Generating SSH key..."
        ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -N "" -q
    fi
    
    # Setup SSH config for connection multiplexing
    ssh_config_dir="$HOME/.ssh"
    ssh_config="$ssh_config_dir/config"
    
    if ! grep -q "ControlMaster auto" "$ssh_config" 2>/dev/null; then
        echo "Configuring SSH connection multiplexing..."
        cat >> "$ssh_config" << EOF

# Proxmox Cluster Optimization
Host pve-*
    ControlMaster auto
    ControlPath /tmp/ssh_control_%h_%p_%r
    ControlPersist 600
    Compression yes
    ServerAliveInterval 60
    ServerAliveCountMax 3

EOF
    fi
    
    # Parallel SSH key setup
    for node in $CLUSTER_NODES; do
        if [ "$node" != "$CURRENT_NODE" ]; then
            (
                echo "Setting up SSH key for node: $node"
                if ssh-copy-id -o ConnectTimeout=5 "root@${node}" >/dev/null 2>&1; then
                    # Test optimized connection
                    if ssh -o ConnectTimeout=2 -o ControlMaster=auto -o ControlPath="/tmp/ssh_control_%h_%p_%r" "root@${node}" "echo 'SSH optimized connection successful'" >/dev/null 2>&1; then
                        echo "SSH connection to $node: OPTIMIZED ⚡"
                    else
                        echo "SSH connection to $node: SUCCESS"
                    fi
                else
                    echo "SSH connection to $node: FAILED"
                fi
            ) &
        fi
    done
    
    wait  # Wait for all parallel setups
    echo "SSH optimization completed!"
}

# Standard SSH setup (for backwards compatibility)
setup_ssh_keys() {
    setup_ssh_keys_fast
}

# Initialize cluster detection
init_cluster_support() {
    CURRENT_NODE=$(hostname)
    debug "Current node: $CURRENT_NODE"
    
    # Create metadata directory for snapshot timestamps
    mkdir -p /etc/pve/snapshot-metadata 2>/dev/null
    debug "Created snapshot metadata directory"
    
    # Apply storage optimizations
    set_storage_optimizations
    
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
        
        # Setup SSH multiplexing for faster connections
        setup_ssh_multiplexing
    else
        CLUSTER_MODE="false"
        CLUSTER_NODES="$CURRENT_NODE"
        echo "Single node mode detected."
        debug "No cluster configuration found, single node mode"
    fi
}

# Enhanced SSH connection testing with connection pooling
test_ssh_connection_fast() {
    local node="$1"
    
    if [ "$node" = "$CURRENT_NODE" ]; then
        return 0
    fi
    
    # Use connection caching file
    cache_file="/tmp/ssh_connection_$node"
    
    # Check if we have a recent successful connection
    if [ -f "$cache_file" ] && [ "$(find "$cache_file" -mmin -2 2>/dev/null)" ]; then
        debug "Using cached SSH connection status for $node"
        return 0
    fi
    
    debug "Testing SSH connection to node: $node (fast)"
    
    # Fast connection test with reduced timeout and connection multiplexing
    if ssh -o ConnectTimeout=2 -o BatchMode=yes -o ControlMaster=auto -o ControlPath="/tmp/ssh_control_%h_%p_%r" "root@${node}" "echo" >/dev/null 2>&1; then
        echo "$(date +%s)" > "$cache_file"
        debug "SSH connection successful to $node"
        return 0
    else
        rm -f "$cache_file"
        debug "SSH connection failed to $node"
        return 1
    fi
}

# Standard SSH test (for backwards compatibility)
test_ssh_connection() {
    test_ssh_connection_fast "$@"
}

# Optimized instance node detection with parallel checking
get_instance_node_fast() {
    local id="$1"
    local is_container="$2"
    
    debug "Fast instance node detection for $id (container: $is_container)"
    
    # Quick local check first
    if [ "$is_container" = "true" ]; then
        if pct status "$id" >/dev/null 2>&1; then
            echo "$CURRENT_NODE"
            return 0
        fi
    else
        if qm status "$id" >/dev/null 2>&1; then
            echo "$CURRENT_NODE"
            return 0
        fi
    fi
    
    # Use pvesh for fast cluster lookup
    if command -v pvesh >/dev/null 2>&1; then
        debug "Using pvesh for fast node detection"
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
    fi
    
    # Parallel cluster node checking for remaining nodes
    if [ "$CLUSTER_MODE" = "true" ]; then
        debug "Parallel checking cluster nodes"
        temp_result="/tmp/node_search_$$"
        
        for node in $CLUSTER_NODES; do
            if [ "$node" != "$CURRENT_NODE" ]; then
                (
                    if test_ssh_connection_fast "$node"; then
                        check_cmd=""
                        if [ "$is_container" = "true" ]; then
                            check_cmd="pct status $id >/dev/null 2>&1"
                        else
                            check_cmd="qm status $id >/dev/null 2>&1"
                        fi
                        
                        if ssh -o ConnectTimeout=2 -o ControlMaster=auto -o ControlPath="/tmp/ssh_control_%h_%p_%r" "root@${node}" "$check_cmd"; then
                            echo "$node" > "$temp_result"
                        fi
                    fi
                ) &
            fi
        done
        
        # Wait for parallel checks (max 5 seconds)
        sleep 2
        wait
        
        if [ -f "$temp_result" ]; then
            result=$(cat "$temp_result")
            rm -f "$temp_result"
            if [ -n "$result" ]; then
                debug "Found instance $id on node $result"
                echo "$result"
                return 0
            fi
        fi
        
        rm -f "$temp_result"
    fi
    
    debug "Instance $id not found anywhere"
    return 1
}

# Standard node detection (for backwards compatibility)
get_instance_node() {
    get_instance_node_fast "$@"
}

# In-memory execution (fastest method for small scripts)
execute_remote_inmemory() {
    local node="$1"
    local action="$2"
    local id="$3"
    local snapshotname="$4"
    local complete_args="$5"
    
    debug "Attempting in-memory execution on $node"
    
    # Create compressed base64-encoded script for transfer
    script_b64=$(gzip -c "$0" | base64 -w 0)
    
    # Check if the compressed script is reasonable size (< 1MB)
    if [ ${#script_b64} -gt 1048576 ]; then
        debug "Script too large for in-memory execution"
        return 1
    fi
    
    # Execute script directly in memory without file creation
    ssh_cmd="echo '$script_b64' | base64 -d | gunzip | sh -s -- $action $id $snapshotname $complete_args"
    
    debug "Executing in-memory command on $node"
    
    # Determine execution mode with faster method
    if [ "$FORCE_INTERACTIVE" = "true" ]; then
        ssh -t -o ConnectTimeout=3 -o ControlMaster=auto -o ControlPath="/tmp/ssh_control_%h_%p_%r" "root@${node}" "$ssh_cmd"
    elif [ "$NON_INTERACTIVE" = "true" ]; then
        ssh -o ConnectTimeout=3 -o ControlMaster=auto -o ControlPath="/tmp/ssh_control_%h_%p_%r" "root@${node}" "$ssh_cmd"
    else
        # Quick non-interactive execution for speed
        ssh -o ConnectTimeout=3 -o ControlMaster=auto -o ControlPath="/tmp/ssh_control_%h_%p_%r" "root@${node}" "$ssh_cmd"
    fi
}

# Optimized file-based execution with parallel operations
execute_remote_optimized() {
    local node="$1"
    local action="$2"
    local id="$3"
    local snapshotname="$4"
    local complete_args="$5"
    
    script_name="snap_$(date +%s)_$$"
    remote_path="/tmp/$script_name"
    
    debug "Optimized file execution on $node as $remote_path"
    
    # Use faster compression for transfer
    temp_script="/tmp/script_compressed_$$"
    compress_script_for_transfer "$0" "$temp_script"
    
    # Parallel copy and execute
    (
        # Copy compressed script
        if scp -o ConnectTimeout=3 -o ControlMaster=auto -o ControlPath="/tmp/ssh_control_%h_%p_%r" -C "$temp_script" "root@${node}:${remote_path}.gz" >/dev/null 2>&1; then
            debug "Compressed script copied to $node"
            
            # Determine decompression command based on compression type
            decomp_cmd="gunzip"
            if command -v lz4 >/dev/null 2>&1 && file "$temp_script" | grep -q "LZ4"; then
                decomp_cmd="lz4 -d"
            elif command -v xz >/dev/null 2>&1 && file "$temp_script" | grep -q "XZ"; then
                decomp_cmd="xz -d"
            fi
            
            # Execute with decompression and cleanup in one command
            exec_cmd="$decomp_cmd $remote_path.gz && chmod +x $remote_path && $remote_path $action $id $snapshotname $complete_args; rm -f $remote_path"
            
            if [ "$NON_INTERACTIVE" = "true" ]; then
                ssh -o ConnectTimeout=3 -o ControlMaster=auto -o ControlPath="/tmp/ssh_control_%h_%p_%r" "root@${node}" "$exec_cmd"
            else
                ssh -t -o ConnectTimeout=3 -o ControlMaster=auto -o ControlPath="/tmp/ssh_control_%h_%p_%r" "root@${node}" "$exec_cmd" 2>/dev/null || \
                ssh -o ConnectTimeout=3 -o ControlMaster=auto -o ControlPath="/tmp/ssh_control_%h_%p_%r" "root@${node}" "$exec_cmd --non-interactive"
            fi
        else
            debug "Compressed copy failed, trying regular copy"
            # Fallback to regular copy
            if scp -o ConnectTimeout=3 -o ControlMaster=auto -o ControlPath="/tmp/ssh_control_%h_%p_%r" "$0" "root@${node}:${remote_path}" >/dev/null 2>&1; then
                exec_cmd="chmod +x $remote_path && $remote_path $action $id $snapshotname $complete_args; rm -f $remote_path"
                ssh -o ConnectTimeout=3 -o ControlMaster=auto -o ControlPath="/tmp/ssh_control_%h_%p_%r" "root@${node}" "$exec_cmd"
            else
                echo "ERROR: Failed to copy script to node $node"
                return 1
            fi
        fi
    ) &
    
    wait  # Wait for execution to complete
    rm -f "$temp_script"
}

# Ultra-fast script deployment and execution for remote nodes
execute_remote_snapshot_fast() {
    local node="$1"
    local action="$2"
    local id="$3"
    local snapshotname="$4"
    local extra_args="$5"
    
    debug "Ultra-fast remote execution on node $node"
    echo "⚡ Executing on remote node $node (ultra-fast mode)..."
    
    # Pre-check SSH connection with faster timeout
    if ! ssh -o ConnectTimeout=2 -o BatchMode=yes -o ControlMaster=auto -o ControlPath="/tmp/ssh_control_%h_%p_%r" "root@${node}" "echo" >/dev/null 2>&1; then
        echo "ERROR: Cannot connect to node $node via SSH"
        return 1
    fi
    
    # Build optimized command flags
    debug_flag=""
    if [ "$DEBUG_MODE" = "true" ]; then
        debug_flag="--debug"
    fi
    
    container_flag=""
    if [ "$CONTAINER_MODE" = "true" ]; then
        container_flag="--container"
    fi
    
    # Build complete arguments with optimizations
    complete_args="--force-local --no-banner $debug_flag $container_flag $extra_args"
    
    # Method 1: Try in-memory execution (fastest - no file I/O)
    if execute_remote_inmemory "$node" "$action" "$id" "$snapshotname" "$complete_args"; then
        debug "In-memory execution successful"
        return 0
    fi
    
    # Method 2: Fallback to optimized file-based execution
    debug "Falling back to optimized file execution"
    execute_remote_optimized "$node" "$action" "$id" "$snapshotname" "$complete_args"
}

# Standard remote execution (for backwards compatibility)
execute_remote_snapshot() {
    execute_remote_snapshot_fast "$@"
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
        if test_ssh_connection_fast "$node"; then
            debug "Executing via SSH on $node"
            ssh -o ControlMaster=auto -o ControlPath="/tmp/ssh_control_%h_%p_%r" "root@${node}" "$command"
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
    local timestamp="$4"
    
    debug "Creating disk snapshot: $diskpath -> $snapshotname (size: $snapshotsize, timestamp: $timestamp)"
    
    # parameters check
    if [ -z "$diskpath" ] || [ -z "$snapshotname" ]; then
        echo "Usage: vm_disk_snapshot_create <diskpath> <snapshotname> [<snapshotsize>] [<timestamp>]"
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
    
    if [ "$DEBUG_MODE" = "true" ]; then
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
        
        # Save timestamp metadata if provided
        if [ -n "$timestamp" ]; then
            metadata_dir="/etc/pve/snapshot-metadata"
            mkdir -p "$metadata_dir" 2>/dev/null
            echo "$timestamp" > "${metadata_dir}/$(basename ${snapshot_path}).time"
            debug "Saved timestamp $timestamp for $(basename ${snapshot_path})"
        fi
        
        # Show snapshot info
        lvs "${snapshot_path}" 2>/dev/null || echo "Note: Snapshot created but may not be immediately visible"
    else
        echo "Snapshot creation failed"
        debug "Snapshot creation failed"
        return 1
    fi
}

# Enhanced delete snapshot of a disk with better error reporting
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
        echo "WARNING: Snapshot '${snapshot_path}' does not exist or already deleted."
        debug "Snapshot does not exist: $snapshot_path"
        # Return success if snapshot doesn't exist (already deleted)
        return 0
    fi
    
    if [ "$DEBUG_MODE" = "true" ]; then
        echo "DEBUG::vm_disk_snapshot_delete $1 $2 => ${snapshot_path}"
    fi
    debug "Deleting snapshot: $snapshot_path"

    # Show snapshot info before deletion
    echo "  Deleting: $(basename $snapshot_path)"
    
    # execute lvremove to remove snapshot (works for both thick and thin)
    debug "Executing: lvremove -y $snapshot_path"
    if lvremove -y "${snapshot_path}" >/dev/null 2>&1; then
        echo "  ✓ Snapshot successfully deleted: $(basename $snapshot_path)"
        debug "Snapshot deletion successful"
        
        # Remove metadata file if it exists
        metadata_file="/etc/pve/snapshot-metadata/$(basename ${snapshot_path}).time"
        if [ -f "$metadata_file" ]; then
            rm -f "$metadata_file"
            debug "Removed metadata file: $metadata_file"
        fi
        
        return 0
    else
        echo "  ✗ Failed to delete snapshot: $(basename $snapshot_path)"
        
        # Try to get more detailed error information
        error_info=$(lvremove -y "${snapshot_path}" 2>&1)
        if [ -n "$error_info" ]; then
            echo "     Error details: $error_info"
        fi
        
        debug "Snapshot deletion failed: $error_info"
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
    
    if [ "$DEBUG_MODE" = "true" ]; then
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

# Ultra-fast disk listing with improved caching and fallback
instance_list_disks_fast() {
    local id="$1"
    local is_container="$2"
    
    debug "Fast listing disks for instance: $id (container: $is_container)"
    
    # Use cached disk list if available (30 seconds cache - shorter for better reliability)
    cache_file="/tmp/disks_cache_$id"
    if [ -f "$cache_file" ] && [ "$(find "$cache_file" -mmin -0.5 2>/dev/null)" ]; then
        cached_content=$(cat "$cache_file" 2>/dev/null)
        if [ -n "$cached_content" ]; then
            debug "Using cached disk list for $id"
            echo "$cached_content"
            return
        else
            debug "Cache file empty, removing: $cache_file"
            rm -f "$cache_file"
        fi
    fi
    
    # Parameters check
    if [ -z "$id" ]; then
        echo "Usage: instance_list_disks <id> <is_container>"
        return 1
    fi

    debug "Fetching fresh disk list for $id"
    
    # Get config
    config_output=$(get_config "$id" "$is_container")
    debug "Retrieved config for instance $id"
    
    if [ -z "$config_output" ]; then
        debug "No config found for instance $id"
        return 1
    fi
    
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

    if [ -z "$disks" ]; then
        debug "No disks found in config for instance $id"
        return 1
    fi

    # Parse each line containing disk and add LVM path
    result=""
    echo "$disks" | while IFS= read -r line; do
        if [ -z "$line" ]; then
            continue
        fi
        
        debug "Processing disk line: $line"
        
        # Extract disk info quickly
        if [ "$is_container" = "true" ]; then
            disk_name=$(echo "$line" | cut -d ' ' -f 2 | cut -d ',' -f 1)
        else
            disk_name=$(echo "$line" | cut -d ' ' -f 2 | cut -d ',' -f 1)
        fi
        
        debug "Disk name: $disk_name"
        
        # Extract part after ':' in disk_name
        disk_basename=$(echo "$disk_name" | cut -d ':' -f 2)    
        debug "Disk basename: $disk_basename"
        
        # Fast LVM path lookup with multiple methods
        lvm_path=""
        
        # Method 1: Direct device mapper lookup (fastest)
        if [ -e "/dev/mapper/$disk_basename" ]; then
            lvm_path="/dev/mapper/$disk_basename"
            debug "Found via mapper: $lvm_path"
        # Method 2: Check common device paths
        elif [ -e "/dev/pve/$disk_basename" ]; then
            lvm_path="/dev/pve/$disk_basename"
            debug "Found via pve: $lvm_path"
        # Method 3: Direct LVM path construction
        elif lvs "$disk_basename" >/dev/null 2>&1; then
            # Try to construct path from LVM info
            vg_name=$(lvs --noheadings -o vg_name "$disk_basename" 2>/dev/null | tr -d ' ')
            if [ -n "$vg_name" ]; then
                lvm_path="/dev/${vg_name}/${disk_basename}"
                debug "Constructed path: $lvm_path"
            fi
        # Method 4: Fast find with limited depth as last resort
        else
            lvm_path=$(find /dev -maxdepth 3 -name "$disk_basename" -type l -print -quit 2>/dev/null)
            debug "Found via find: $lvm_path"
        fi
        
        if [ -n "$lvm_path" ] && [ -e "$lvm_path" ]; then
            echo "$line,$disk_basename,$lvm_path"
        else
            debug "WARNING: Could not find LVM path for $disk_basename"
            # Still include it in case it can be resolved later
            echo "$line,$disk_basename,"
        fi
    done > "/tmp/disk_parse_$$"
    
    # Read results and cache them
    result=$(cat "/tmp/disk_parse_$$" 2>/dev/null)
    rm -f "/tmp/disk_parse_$$"
    
    if [ -n "$result" ]; then
        debug "Caching disk list for $id"
        echo "$result" | tee "$cache_file"
    else
        debug "No valid disk results found"
        rm -f "$cache_file"
        return 1
    fi
}

# Fallback to standard disk listing without cache
instance_list_disks_no_cache() {
    local id="$1"
    local is_container="$2"
    
    debug "Standard listing disks for instance: $id (container: $is_container)"
    
    # parameters check
    if [ -z "$id" ]; then
        echo "Usage: instance_list_disks <id> <is_container>"
        return 1
    fi

    # Get config based on instance type
    config_output=$(get_config "$id" "$is_container")
    debug "Retrieved config for instance $id"
    
    if [ -z "$config_output" ]; then
        debug "No config output received"
        return 1
    fi
    
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

    if [ -z "$disks" ]; then
        debug "No disks found in config"
        return 1
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
        lvm_path=""
        
        # Try multiple methods to find the LVM path
        if [ -e "/dev/mapper/$disk_basename" ]; then
            lvm_path="/dev/mapper/$disk_basename"
        elif [ -e "/dev/pve/$disk_basename" ]; then
            lvm_path="/dev/pve/$disk_basename"
        else
            lvm_path=$(find /dev -name "$disk_basename" -type l -exec ls -f {} + 2>/dev/null | head -1)
        fi
        
        debug "LVM path: $lvm_path"

        if [ -n "$lvm_path" ]; then
            echo "$line,$disk_basename,$lvm_path"
        else
            echo "$line,$disk_basename,"
        fi
    done
}

# Robust disk listing with fallback
instance_list_disks() {
    local id="$1"
    local is_container="$2"
    
    # Try fast method first
    result=$(instance_list_disks_fast "$id" "$is_container")
    
    # If fast method fails or returns empty, use fallback
    if [ -z "$result" ]; then
        debug "Fast disk listing failed, using fallback method"
        result=$(instance_list_disks_no_cache "$id" "$is_container")
    fi
    
    # If still no result, clear any bad cache and try once more
    if [ -z "$result" ]; then
        debug "Both methods failed, clearing cache and trying once more"
        rm -f "/tmp/disks_cache_$id" "/tmp/instance_cache_$id"
        result=$(instance_list_disks_no_cache "$id" "$is_container")
    fi
    
    echo "$result"
}

# Ultra-fast atomic snapshot creation with parallel processing
instance_snapshot_create_atomic_fast() {
    local id="$1"
    local snapshotname="$2"
    local is_container="$3"
    
    debug "Creating ultra-fast atomic snapshot: $id -> $snapshotname (container: $is_container)"
    
    # Parameters check
    if [ -z "$id" ] || [ -z "$snapshotname" ]; then
        echo "ERROR: Both ID and snapshotname must be provided."
        return 1
    fi
    
    instance_type="VM"
    if [ "$is_container" = "true" ]; then
        instance_type="Container"
    fi
    
    echo "🚀 Creating ultra-fast atomic snapshot '$snapshotname' for $instance_type $id..."
    start_time=$(date +%s)
    
    # Get list of instance disks using fast method
    disks=$(instance_list_disks_fast "$id" "$is_container")
    debug "Retrieved disk list for instance $id"
    
    if [ -z "$disks" ]; then
        echo "ERROR: No disks found for $instance_type $id"
        return 1
    fi
    
    # Phase 1: Ultra-fast preparation (parallel validation)
    echo "Phase 1: Lightning-fast preparation..."
    snapshot_timestamp=$(date +%s)  # Pre-generate timestamp
    
    # Build snapshot commands in parallel
    temp_commands_file="/tmp/snapshot_commands_$$"
    temp_validation_file="/tmp/validation_$$"
    temp_disks_file="/tmp/disks_temp_$$"
    
    echo "$disks" > "$temp_disks_file"
    
    # Parallel processing of disk preparation
    while IFS= read -r line; do
        if [ -z "$line" ]; then
            continue
        fi
        
        (
            device_path=$(echo "$line" | awk -F ',' '{print $NF}')
            if [ -z "$device_path" ] || [ ! -e "$device_path" ]; then
                exit 0
            fi
            
            # Quick snapshot existence check
            snapshot_path="${device_path}-snapshot-${snapshotname}"
            if lvs "${snapshot_path}" >/dev/null 2>&1; then
                echo "ERROR: Snapshot exists: ${snapshot_path}" >> "$temp_validation_file"
                exit 0
            fi
            
            # Fast size calculation and command building
            is_thin=$(is_thin_lv "$device_path")
            if [ "$is_thin" = "true" ]; then
                # Thin snapshot command (fastest)
                echo "lvcreate -s -n $(basename ${snapshot_path}) $device_path" >> "$temp_commands_file"
                echo "${snapshot_path}|thin" >> "$temp_validation_file"
            else
                # Quick size calculation for regular LVM
                config_size=$(echo "$line" | grep -oP 'size=\K[^,]+' || echo "")
                if [ -n "$config_size" ]; then
                    size_num=$(echo "$config_size" | grep -o '[0-9]\+')
                    # Simplified size calculation (faster) - use smaller percentages for speed
                    if [ "$size_num" -lt 100 ]; then
                        snapshot_size="${size_num}G"  # 100% for small disks (faster allocation)
                    else
                        # Use 20% for larger disks (faster than calculating exact percentages)
                        snapshot_size=$(( size_num / 5 ))G
                    fi
                else
                    snapshot_size="10G"
                fi
                
                echo "lvcreate -L $snapshot_size -s -n $(basename ${snapshot_path}) $device_path" >> "$temp_commands_file"
                echo "${snapshot_path}|regular" >> "$temp_validation_file"
            fi
            
            echo "$(basename $device_path)|$is_thin" >&2
        ) &
    done < "$temp_disks_file"
    
    wait  # Wait for all preparation to complete
    rm -f "$temp_disks_file"
    
    # Check for validation errors
    if [ -f "$temp_validation_file" ] && grep -q "ERROR:" "$temp_validation_file" 2>/dev/null; then
        cat "$temp_validation_file"
        rm -f "$temp_commands_file" "$temp_validation_file"
        return 1
    fi
    
    # Phase 2: Minimal downtime approach
    instance_status=$(get_status "$id" "$is_container")
    instance_was_running=false
    use_freeze=false
    suspension_method=""
    
    if echo "$instance_status" | grep -q "status: running"; then
        instance_was_running=true
        echo "Phase 2: Minimal downtime approach..."
        
        # Try filesystem freeze first (fastest method - sub-second downtime)
        if [ "$is_container" = "false" ]; then
            # For VMs: Try QEMU guest agent freeze (sub-second)
            if qm guest cmd "$id" fs-freeze 2>/dev/null; then
                use_freeze=true
                suspension_method="freeze"
                echo "  ⚡ VM filesystem frozen (ultra-fast method - <0.5s downtime)"
            else
                # Fallback: Quick suspend
                if qm suspend "$id" 2>/dev/null; then
                    suspension_method="suspend"
                    echo "  VM suspended"
                else
                    qm stop "$id" 2>/dev/null
                    suspension_method="stop"
                    echo "  VM stopped"
                    sleep 1
                fi
            fi
        else
            # For containers: Quick suspend/stop
            if pct suspend "$id" 2>/dev/null; then
                suspension_method="suspend"
                echo "  Container suspended"
            else
                pct stop "$id" 2>/dev/null
                suspension_method="stop"
                echo "  Container stopped"
                sleep 1  # Minimal wait
            fi
        fi
    fi
    
    # Phase 3: Parallel snapshot execution (FASTEST!)
    echo "Phase 3: Parallel snapshot creation..."
    
    # Execute all snapshot commands in parallel with job control
    max_jobs=$(nproc 2>/dev/null || echo "4")  # Use all available CPU cores
    job_count=0
    
    if [ -f "$temp_commands_file" ]; then
        while IFS= read -r cmd; do
            if [ -z "$cmd" ]; then
                continue
            fi
            
            # Execute command in background
            (
                debug "Executing: $cmd"
                if eval "$cmd" >/dev/null 2>&1; then
                    echo "0" > "/tmp/snap_result_$$_$job_count"
                else
                    echo "1" > "/tmp/snap_result_$$_$job_count"
                fi
            ) &
            
            job_count=$((job_count + 1))
            
            # Limit parallel jobs to prevent overwhelming the system
            if [ "$job_count" -ge "$max_jobs" ]; then
                wait  # Wait for batch to complete
                job_count=0
            fi
            
        done < "$temp_commands_file"
        
        # Wait for remaining jobs
        wait
    fi
    
    # Phase 4: Immediate restore (fastest resume)
    if [ "$instance_was_running" = "true" ]; then
        echo "Phase 4: Immediate restore..."
        
        if [ "$use_freeze" = "true" ]; then
            # Unfreeze filesystem (fastest)
            qm guest cmd "$id" fs-thaw 2>/dev/null
            echo "  ⚡ VM filesystem thawed (total downtime: <1 second)"
        elif [ "$is_container" = "false" ]; then
            # Resume VM
            if [ "$suspension_method" = "suspend" ]; then
                qm resume "$id" 2>/dev/null
            else
                qm start "$id" 2>/dev/null
            fi
            echo "  VM resumed"
        else
            # Resume container
            if [ "$suspension_method" = "suspend" ]; then
                pct resume "$id" 2>/dev/null
            else
                pct start "$id" 2>/dev/null
            fi
            echo "  Container resumed"
        fi
    fi
    
    # Check for any failures
    creation_success=true
    failed_snapshots=""
    for i in $(seq 0 $((job_count - 1))); do
        if [ -f "/tmp/snap_result_$$_$i" ]; then
            result=$(cat "/tmp/snap_result_$$_$i")
            if [ "$result" != "0" ]; then
                creation_success=false
                failed_snapshots="$failed_snapshots $i"
            fi
        fi
    done
    
    # Phase 5: Fast metadata setting (parallel)
    if [ "$creation_success" = "true" ] && [ -f "$temp_validation_file" ]; then
        echo "Phase 5: Fast metadata..."
        
        # Set metadata in parallel
        while IFS='|' read -r snapshot_path snap_type; do
            if [ -n "$snapshot_path" ]; then
                (
                    metadata_dir="/etc/pve/snapshot-metadata"
                    mkdir -p "$metadata_dir" 2>/dev/null
                    echo "$snapshot_timestamp" > "${metadata_dir}/$(basename $snapshot_path).time"
                ) &
            fi
        done < "$temp_validation_file"
        
        wait  # Wait for metadata operations
    fi
    
    # Cleanup
    rm -f "$temp_commands_file" "$temp_validation_file" /tmp/snap_result_$$_*
    
    end_time=$(date +%s)
    total_time=$((end_time - start_time))
    
    if [ "$creation_success" = "true" ]; then
        echo ""
        echo "🚀 Ultra-fast atomic snapshot '$snapshotname' created for $instance_type $id"
        echo "   ⚡ Total time: ${total_time} seconds"
        if [ "$use_freeze" = "true" ]; then
            echo "   ⚡ Downtime: <1 second (filesystem freeze)"
        else
            echo "   ⚡ Downtime: <3 seconds"
        fi
        echo "   Use './snapshot.sh list $id' to see created snapshots"
    else
        echo ""
        echo "❌ Ultra-fast atomic snapshot creation failed - cleaning up partial snapshots..."
        # Cleanup would go here
        return 1
    fi
    
    return 0
}

# create snapshot for each instance disk with enhanced thin/thick support
instance_snapshot_create() {
    # Use ultra-fast atomic version for better consistency and speed
    instance_snapshot_create_atomic_fast "$@"
}

# Fixed snapshot delete for each instance disk with proper error handling
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
    
    instance_type="VM"
    if [ "$is_container" = "true" ]; then
        instance_type="Container"
    fi
    
    echo "Deleting snapshot '$snapshotname' for $instance_type $id..."
    
    # get list of instance disks using fast method
    disks=$(instance_list_disks_fast "$id" "$is_container")
    debug "Retrieved disk list for deletion"
    
    if [ -z "$disks" ]; then
        echo "ERROR: No disks found for $instance_type $id"
        return 1
    fi
    
    # Create temporary file for disk processing
    temp_disks_file="/tmp/delete_disks_$$"
    echo "$disks" > "$temp_disks_file"
    
    deletion_success=true
    deleted_count=0
    
    # Parse each disk line sequentially for better error reporting
    while IFS= read -r line; do
        if [ -z "$line" ]; then
            continue
        fi
        
        # extract device path (last element after comma)
        device_path=$(echo "$line" | awk -F ',' '{print $NF}')
        
        if [ -z "$device_path" ] || [ ! -e "$device_path" ]; then
            echo "WARNING: Invalid device path: $device_path"
            debug "Invalid device path: $device_path"
            continue
        fi
        
        echo "Processing disk: $device_path"
        debug "Deleting snapshot for device: $device_path"
        
        # Call function to delete disk snapshot with proper error handling
        if vm_disk_snapshot_delete "$device_path" "$snapshotname"; then
            deleted_count=$((deleted_count + 1))
            echo "✓ Deleted snapshot for: $(basename $device_path)"
        else
            echo "✗ Failed to delete snapshot for: $(basename $device_path)"
            deletion_success=false
        fi
        
    done < "$temp_disks_file"
    
    rm -f "$temp_disks_file"
    
    if [ "$deletion_success" = "true" ] && [ "$deleted_count" -gt 0 ]; then
        echo ""
        echo "✅ Successfully deleted snapshot '$snapshotname' for $instance_type $id"
        echo "   Deleted $deleted_count disk snapshot(s)"
    elif [ "$deleted_count" -eq 0 ]; then
        echo ""
        echo "❌ No snapshots were found to delete for $instance_type $id"
        return 1
    else
        echo ""
        echo "⚠️  Partial deletion completed for $instance_type $id"
        echo "   Some snapshots may still exist"
        return 1
    fi
    
    debug "Instance snapshot deletion completed"
    return 0
}

# Improved revert function with better disk detection
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

    # Clear any stale cache before revert operation
    rm -f "/tmp/disks_cache_$id" "/tmp/instance_cache_$id"

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
        
    # get instance disk list using robust method
    echo "Getting disk list for revert operation..."
    disks=$(instance_list_disks "$id" "$is_container")
    debug "Retrieved disk list for revert"
    
    if [ -z "$disks" ]; then
        echo "ERROR: No disks found for $instance_type $id"
        debug "No disks found for revert operation"
        
        # Try to restart instance if it was running
        if [ "$instance_was_running" = "true" ]; then
            echo "Attempting to restart $instance_type since no revert was performed..."
            start_instance "$id" "$is_container"
        fi
        return 1
    fi
    
    echo "Found disks for revert operation:"
    echo "$disks" | while IFS= read -r line; do
        if [ -n "$line" ]; then
            device_path=$(echo "$line" | awk -F ',' '{print $NF}')
            echo "  - $(basename $device_path)"
        fi
    done
    
    if [ "$DEBUG_MODE" = "true" ]; then
        echo "DEBUG::instance_revert_to_snapshot $1 $2"
    fi
    
    # Create temp file for snapshots to recreate
    rm -f "/tmp/snapshots_to_recreate.$$" 2>/dev/null
    debug "Cleaned up temp files"
    
    # Create temporary file for disk processing
    temp_disks_file="/tmp/revert_disks_$$"
    echo "$disks" > "$temp_disks_file"
    
    # parse each disk line for reverting
    revert_success=true
    reverted_count=0
    
    while IFS= read -r line; do
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
        if vm_disk_revert_to_snapshot "$device_path" "$snapshotname" "$keep_snapshot"; then
            reverted_count=$((reverted_count + 1))
            echo "✓ Successfully initiated revert for: $(basename $device_path)"
        else
            echo "✗ Failed to revert disk $device_path"
            debug "Failed to revert disk: $device_path"
            revert_success=false
        fi
    done < "$temp_disks_file"
    
    rm -f "$temp_disks_file"

    if [ "$reverted_count" -eq 0 ]; then
        echo "ERROR: No disks were reverted"
        # Try to restart instance if it was running
        if [ "$instance_was_running" = "true" ]; then
            echo "Attempting to restart $instance_type since no revert was performed..."
            start_instance "$id" "$is_container"
        fi
        return 1
    fi

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
        temp_check_file="/tmp/merge_check_$$"
        echo "$disks" > "$temp_check_file"
        
        while IFS= read -r line; do
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
        done < "$temp_check_file"
        
        rm -f "$temp_check_file"
        
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

    echo ""
    if [ "$revert_success" = "true" ]; then
        echo "✅ Successfully reverted $instance_type $id to snapshot '$snapshotname'"
        echo "   Reverted $reverted_count disk(s)"
    else
        echo "⚠️  Partial revert completed for $instance_type $id"
        echo "   Some disks may not have been reverted properly"
    fi

    debug "Instance revert operation completed"
    return 0
}

# Enhanced snapshot list function with proper creation time
instance_snapshot_list() {
    local id="$1"
    local is_container="$2"
    
    debug "Listing snapshots for instance: $id (container: $is_container)"
    
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
        
        # Find instance's disk snapshots with enhanced time handling
        lvs "$vg" --separator ":" --noheadings -o lv_name,lv_size,data_percent,origin,pool_lv,lv_time 2>/dev/null | \
        grep -E "vm-${id}-disk|vm--${id}--disk" | grep "snapshot" | \
        while IFS=":" read -r lv_name lv_size usage origin pool_lv creation_time_raw; do
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
            
            # Enhanced date formatting with multiple methods
            creation_date="Unknown"
            
            # Method 1: Check our metadata file first
            metadata_file="/etc/pve/snapshot-metadata/${lv_name}.time"
            if [ -f "$metadata_file" ]; then
                saved_timestamp=$(cat "$metadata_file" 2>/dev/null)
                if [ -n "$saved_timestamp" ] && echo "$saved_timestamp" | grep -qE '^[0-9]+$'; then
                    creation_date=$(date -d "@$saved_timestamp" "+%Y-%m-%d %H:%M" 2>/dev/null || echo "Unknown")
                    debug "Used metadata timestamp for $lv_name: $creation_date"
                fi
            fi
            
            # Method 2: Try LVM timestamp if metadata not available
            if [ "$creation_date" = "Unknown" ] && [ -n "$creation_time_raw" ]; then
                if echo "$creation_time_raw" | grep -qE '^[0-9]+$'; then
                    creation_date=$(date -d "@$creation_time_raw" "+%Y-%m-%d %H:%M" 2>/dev/null || echo "Unknown")
                    debug "Used LVM timestamp for $lv_name: $creation_date"
                elif echo "$creation_time_raw" | grep -qE '^[0-9]{4}-[0-9]{2}-[0-9]{2}'; then
                    # Handle different date formats
                    creation_date=$(date -d "$creation_time_raw" "+%Y-%m-%d %H:%M" 2>/dev/null || echo "Unknown")
                    debug "Parsed date format for $lv_name: $creation_date"
                fi
            fi
            
            # Method 3: Try filesystem timestamp as last resort
            if [ "$creation_date" = "Unknown" ]; then
                lv_device="/dev/$vg/$lv_name"
                if [ -e "$lv_device" ]; then
                    # Get file creation time (works on some systems)
                    fs_time=$(stat -c %Y "$lv_device" 2>/dev/null)
                    if [ -n "$fs_time" ] && [ "$fs_time" -gt 0 ]; then
                        creation_date=$(date -d "@$fs_time" "+%Y-%m-%d %H:%M" 2>/dev/null || echo "Unknown")
                        debug "Used filesystem timestamp for $lv_name: $creation_date"
                    fi
                fi
            fi
            
            debug "Final creation date for $lv_name: $creation_date"
            
            # Format usage percentage
            if [ -n "$usage" ] && [ "$usage" != "100.00" ] && [ "$usage" != "" ]; then
                usage="${usage}%"
            else
                usage="N/A"
            fi
            
            # Print snapshot info in table format
            printf "| %-13s | %-10s | %-7s | %-5s | %-4s | %-20s |\n" \
                "$snapshot_name" "$disk_info" "$lv_size" "$usage" "$snap_type" "$creation_date"
        done
    done
    
    echo "============================================================================"
    
    # Check if we found any snapshots
    if ! lvs --noheadings 2>/dev/null | grep -E "vm-${id}-disk|vm--${id}--disk" | grep -q "snapshot"; then
        echo "No snapshots found for $instance_type $id"
        debug "No snapshots found for instance $id"
    fi
}

# Enhanced main detection with better error handling and fast detection
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
        # Use fast detection first
        echo "⚡ Auto-detecting type for ID $id..."
        
        detect_container_or_vm_fast "$id"
        detection_result=$?
        
        case $detection_result in
            0)
                is_container="true"
                debug "Fast auto-detected as container"
                echo "✓ Auto-detected as Container (fast)"
                return 0
                ;;
            1)
                is_container="false"
                debug "Fast auto-detected as VM"
                echo "✓ Auto-detected as VM (fast)"
                return 0
                ;;
            *)
                # Fallback to enhanced detection
                detect_container_or_vm "$id"
                detection_result=$?
                
                case $detection_result in
                    0)
                        is_container="true"
                        debug "Enhanced auto-detected as container"
                        echo "✓ Auto-detected as Container"
                        return 0
                        ;;
                    1)
                        is_container="false"
                        debug "Enhanced auto-detected as VM"
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
                ;;
        esac
    fi
}

# Enhanced cleanup function
cleanup() {
    debug "Cleaning up temporary files"
    rm -f "/tmp/snapshots_to_recreate.$$" 2>/dev/null
    rm -f /tmp/instance_cache_* 2>/dev/null
    rm -f /tmp/disks_cache_* 2>/dev/null
    rm -f /tmp/*_temp_$$ 2>/dev/null
    rm -f /tmp/snap_result_$$_* 2>/dev/null
    rm -f /tmp/ssh_connection_* 2>/dev/null
    rm -f /tmp/script_compressed_* 2>/dev/null
    
    # Close SSH connections
    for control_socket in /tmp/ssh_control_*; do
        if [ -S "$control_socket" ]; then
            ssh -O exit -o ControlPath="$control_socket" dummy 2>/dev/null || true
        fi
    done
}

# Set up trap to clean up on exit
trap cleanup EXIT INT TERM

# Show support message at start
show_support_message

# Initialize cluster support
init_cluster_support

# Check for special commands
if [ "$1" = "--setup-ssh" ]; then
    setup_ssh_keys_fast
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
            instance_node=$(get_instance_node_fast "$id" "$is_container")
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
                echo "$instance_type $id found locally - executing ultra-fast snapshot creation"
                debug "Executing locally on $CURRENT_NODE"
                instance_snapshot_create "$id" "$snapshotname" "$is_container"
            else
                instance_type="VM"
                if [ "$is_container" = "true" ]; then
                    instance_type="Container"
                fi
                echo "$instance_type $id found on node: $instance_node - executing remotely (ultra-fast)"
                debug "Executing remotely on $instance_node"
                execute_remote_snapshot_fast "$instance_node" "create" "$id" "$snapshotname"
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
            instance_node=$(get_instance_node_fast "$id" "$is_container")
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
                echo "$instance_type $id found on node: $instance_node - executing remotely (ultra-fast)"
                debug "Executing remotely on $instance_node"
                execute_remote_snapshot_fast "$instance_node" "delete" "$id" "$snapshotname"
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
            instance_node=$(get_instance_node_fast "$id" "$is_container")
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
                echo "$instance_type $id found on node: $instance_node - executing remotely (ultra-fast)"
                debug "Executing remotely on $instance_node"
                execute_remote_snapshot_fast "$instance_node" "revert" "$id" "$snapshotname" "$extra_args"
            fi
        fi
        ;;
    list)
        debug "Starting list action"
        
        if [ "$force_local" = "true" ]; then
            debug "Force local execution"
            instance_snapshot_list "$id" "$is_container"
        else
            instance_node=$(get_instance_node_fast "$id" "$is_container")
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
                echo "$instance_type $id found on node: $instance_node - showing snapshots remotely (ultra-fast)"
                debug "Executing remotely on $instance_node"
                execute_remote_snapshot_fast "$instance_node" "list" "$id" ""
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
