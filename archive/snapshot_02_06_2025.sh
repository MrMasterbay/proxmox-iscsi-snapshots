#!/bin/sh
#
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
#
# Rewritten by Nico Schmidt (baGStube_Nico)
# E-Mail: nico.schmidt@ns-tech.cloud
# Follow my Socials: https://linktr.ee/bagstube_nico

# 
usage() {
    echo "Usage: $0 <action> <vmid> [<snapshotname>] [options]"
    echo "  action       : Action (list, create, delete, revert)"
    echo "  vmid         : VMID "
    echo "  snapshotname : snapshot name (mandatory for action create,delete,revert)"
    echo ""
    echo "Options for revert action:"
    echo "  --no-autostart : Do not start VM after revert (VM starts by default)"
}

# create a snapshot of a disk
vm_disk_snapshot_create() {
    local diskpath="$1"
    local snapshotname="$2"
    local snapshotsize="$3"
    
    # parameters check
    if [ -z "$diskpath" ] || [ -z "$snapshotname" ] || [ -z "$snapshotsize" ]; then
        echo "Usage: vm_disk_snapshot_create <diskpath> <snapshotname> <snapshotsize>"
        return 1
    fi

    # check of diskpath existence
    if [ ! -e "$diskpath" ]; then
        echo "ERROR : disk path '$diskpath' not exists."
        return 1
    fi
    
    echo "DEBUG::vm_disk_snapshot_create $1 $2 $3 => ${diskpath}-snapshot-${snapshotname}"
    #return 1

    # execute lvcreate command to create snapshot
    lvcreate -L "+${snapshotsize}" -s -n "${diskpath}-snapshot-${snapshotname}" "${diskpath}"
    
    # check if command succeeded
    if [ $? -eq 0 ]; then
        echo "Snapshot sucessfully created : ${diskpath}-snapshot-${snapshotname}"
    else
        echo "Snapshot creation failed"
        return 1
    fi
}

# delete snapshot of a disk
vm_disk_snapshot_delete() {
    local diskpath="$1"
    local snapshotname="$2"
    
    # parameters check
    if [ -z "$diskpath" ] || [ -z "$snapshotname" ]; then
        echo "Usage: vm_disk_snapshot_delete <diskpath> <snapshotname>"
        return 1
    fi

    # check of diskpath existence
    if [ ! -e "$diskpath" ]; then
        echo "ERROR : Disk path '$diskpath' not exists."
        return 1
    fi
    
    echo "DEBUG::vm_disk_snapshot_delete $1 $2 => ${diskpath}-snapshot-${snapshotname}"
    #return 1

    # execute lvremove to remove snapshot
    lvremove -y "${diskpath}-snapshot-${snapshotname}"
    
    # check if command succeeded
    if [ $? -eq 0 ]; then
        echo "Snapshot successfully deleted : ${diskpath}-snapshot-${snapshotname}"
    else
        echo "Snapshot delete failed"
        return 1
    fi
}

# create a copy of a snapshot
vm_disk_snapshot_copy() {
    local diskpath="$1"
    local snapshotname="$2"
    local tempname="$3"
    local snapsize="$4"
    
    # parameters check
    if [ -z "$diskpath" ] || [ -z "$snapshotname" ] || [ -z "$tempname" ] || [ -z "$snapsize" ]; then
        echo "Usage: vm_disk_snapshot_copy <diskpath> <snapshotname> <tempname> <snapsize>"
        return 1
    fi
    
    # Check if original snapshot exists
    if [ ! -e "${diskpath}-snapshot-${snapshotname}" ]; then
        echo "ERROR: Original snapshot '${diskpath}-snapshot-${snapshotname}' does not exist."
        return 1
    fi
    
    echo "Creating backup copy of snapshot: ${diskpath}-snapshot-${tempname}"
    
    # Create a new snapshot of the same disk
    lvcreate -L "${snapsize}" -s -n "${diskpath}-snapshot-${tempname}" "${diskpath}"
    
    if [ $? -eq 0 ]; then
        echo "Backup snapshot created: ${diskpath}-snapshot-${tempname}"
        return 0
    else
        echo "Failed to create backup snapshot"
        return 1
    fi
}

# revert a disk to snapshot
vm_disk_revert_to_snapshot() {
    local diskpath="$1"
    local snapshotname="$2"
    local keep_snapshot="$3"
    
    # parameters check
    if [ -z "$diskpath" ] || [ -z "$snapshotname" ]; then
        echo "Usage: vm_disk_revert_to_snapshot <diskpath> <snapshotname>"
        return 1
    fi

    # check of diskpath existence
    if [ ! -e "$diskpath" ]; then
        echo "ERROR : disk path '$diskpath' not exists."
        return 1
    fi
    
    # check of snapshot existence
    if [ ! -e "${diskpath}-snapshot-${snapshotname}" ]; then
        echo "ERROR : snapshot path '${diskpath}-snapshot-${snapshotname}' not exists."
        return 1
    fi
    
    # If we want to keep the snapshot, create a backup copy first
    local temp_snapshot_name=""
    if [ "$keep_snapshot" = "true" ]; then
        temp_snapshot_name="${snapshotname}-temp-$$"
        
        # Get original snapshot size
        local snap_info=$(lvs --noheadings -o lv_size --units m "${diskpath}-snapshot-${snapshotname}" 2>/dev/null)
        if [ -z "$snap_info" ]; then
            echo "WARNING: Could not determine snapshot size, using default"
            snap_info="1000M"
        fi
        local snap_size=$(echo "$snap_info" | tr -d ' ')
        
        # Create backup snapshot
        vm_disk_snapshot_copy "$diskpath" "$snapshotname" "$temp_snapshot_name" "$snap_size"
        
        if [ $? -ne 0 ]; then
            echo "WARNING: Failed to create backup snapshot, original will be lost during revert"
            keep_snapshot="false"
        fi
    fi
    
    echo "DEBUG::vm_disk_revert_to_snapshot $1 $2 => ${diskpath}-snapshot-${snapshotname}"
    #return 1

    # execute lvconvert merge to revert to snapshot
    lvconvert --merge "${diskpath}-snapshot-${snapshotname}"
    
    # check if command succeeded
    if [ $? -eq 0 ]; then
        echo "Revert to snapshot succeded : ${diskpath}-snapshot-${snapshotname}"
        
        # If we're keeping the snapshot, save its info for recreation later
        if [ "$keep_snapshot" = "true" ] && [ -n "$temp_snapshot_name" ]; then
            echo "${diskpath}|${snapshotname}|${temp_snapshot_name}" >> "/tmp/snapshots_to_recreate.$$"
        fi
        
        return 0
    else
        echo "Failed to revert to snapshot ${diskpath}-snapshot-${snapshotname}"
        
        # Clean up temp snapshot if we created one
        if [ "$keep_snapshot" = "true" ] && [ -n "$temp_snapshot_name" ]; then
            lvremove -y "${diskpath}-snapshot-${temp_snapshot_name}" 2>/dev/null
        fi
        
        return 1
    fi
}

# Recreate snapshots after merge completes
recreate_snapshots() {
    local tmp_file="/tmp/snapshots_to_recreate.$$"
    
    if [ ! -f "$tmp_file" ]; then
        return 0
    fi
    
    echo "Recreating preserved snapshots..."
    
    # Process each line in the temp file
    while IFS='|' read -r diskpath snapshotname tempname; do
        # Wait for the temporary snapshot to be fully available
        sleep 2
        
        # Get size of temp snapshot
        local snap_info=$(lvs --noheadings -o lv_size --units m "${diskpath}-snapshot-${tempname}" 2>/dev/null)
        if [ -z "$snap_info" ]; then
            echo "WARNING: Could not determine temp snapshot size, using default"
            snap_info="1000M"
        fi
        local snap_size=$(echo "$snap_info" | tr -d ' ')
        
        echo "Recreating original snapshot '${snapshotname}' from temp copy"
        
        # Create a new snapshot with the original name
        lvcreate -L "${snap_size}" -s -n "${diskpath}-snapshot-${snapshotname}" "${diskpath}"
        
        if [ $? -eq 0 ]; then
            echo "Successfully recreated snapshot: ${diskpath}-snapshot-${snapshotname}"
        else
            echo "Failed to recreate snapshot: ${diskpath}-snapshot-${snapshotname}"
        fi
        
        # Remove the temporary snapshot
        lvremove -y "${diskpath}-snapshot-${tempname}" 2>/dev/null
    done < "$tmp_file"
    
    # Clean up temp file
    rm -f "$tmp_file"
}

# disable lv volume
vm_disk_lv_desactivate() {
    local diskpath="$1"

    # parameters check
    if [ -z "$diskpath" ]; then
        echo "Usage: vm_disk_lv_desactivate <diskpath>"
        return 1
    fi
    
    echo "DEBUG::vm_disk_lv_desactivate $1"
    
    # disable logical volume
    lvchange -an "${diskpath}"    

    # check if command succeeded    
    if [ $? -eq 0 ]; then
        echo "Desactivate lv done : ${diskpath}"
    else
        echo "Failed to desactivate lv : ${diskpath}"
        return 1
    fi
}

# activate lv volume
vm_disk_lv_activate() {
    local diskpath="$1"

    # parameters check
    if [ -z "$diskpath" ]; then
        echo "Usage: vm_disk_lv_activate <diskpath>"
        return 1
    fi
    
    echo "DEBUG::vm_disk_lv_activate $1"
    
    # active logical volume
    lvchange -ay "${diskpath}"    
    
    # check if command succeeded
    if [ $? -eq 0 ]; then
        echo "Activate lv done : ${diskpath}"
    else
        echo "Failed to activate lv : ${diskpath}"
        return 1
    fi
}

# Improved snapshot list function with fixed date display
vm_snapshot_list() {
    local vmid="$1"
    
    # parameters check
    if [ -z "$vmid" ]; then
        echo "Usage: vm_snapshot_list <vmid>"
        return 1
    fi
    
    echo "========================= Snapshots for VM $vmid ========================="
    echo "| Snapshot Name | Disk        | Size    | Usage | Creation Date        |"
    echo "|---------------|-------------|---------|-------|----------------------|"
    
    # Find all volume groups
    for vg in $(vgs --noheadings -o vg_name 2>/dev/null | tr -d ' '); do
        # Find VM's disk snapshots (supporting both hyphen and double-hyphen format)
        lvs "$vg" --separator ":" --noheadings -o lv_name,lv_size,data_percent,origin,lv_time 2>/dev/null | grep -E "vm-${vmid}-disk|vm--${vmid}--disk" | grep "snapshot" | while IFS=":" read -r lv_name lv_size usage origin creation_time_raw; do
            # Clean up whitespace
            lv_name=$(echo "$lv_name" | tr -d ' ')
            lv_size=$(echo "$lv_size" | tr -d ' ')
            usage=$(echo "$usage" | tr -d ' ')
            creation_time_raw=$(echo "$creation_time_raw" | tr -d ' ')
            
            # Extract snapshot name
            snapshot_name=$(echo "$lv_name" | sed -n 's/.*snapshot-\(.*\)/\1/p')
            
            # Extract disk number
            disk_num=$(echo "$lv_name" | grep -oE 'disk-[0-9]+|disk--[0-9]+' | sed -e 's/disk-//' -e 's/disk--//')
            if [ -z "$disk_num" ]; then
                disk_num="unknown"
            fi
            
            # Format creation date - handle various formats
            creation_date="Unknown"
            
            # Try different date conversion approaches
            if [ -n "$creation_time_raw" ]; then
                # Try direct conversion if timestamp is in seconds
                if echo "$creation_time_raw" | grep -qE '^[0-9]+$'; then
                    creation_date=$(date -d "@$creation_time_raw" "+%Y-%m-%d %H:%M" 2>/dev/null)
                fi
                
                # If still unknown, try alternative date formats
                if [ "$creation_date" = "Unknown" ]; then
                    # Get current date info for LVM timestamp interpretation
                    current_year=$(date "+%Y")
                    
                    # Format: "YYYY-MM-DD HH:MM:SS +ZZZZ"
                    if echo "$creation_time_raw" | grep -qE '[0-9]{4}-[0-9]{2}-[0-9]{2}'; then
                        creation_date=$(echo "$creation_time_raw" | cut -d' ' -f1,2 | cut -d':' -f1,2)
                    # Format: "Mmm DD HH:MM:SS YYYY"
                    elif echo "$creation_time_raw" | grep -qE '[A-Za-z]{3} [0-9]{1,2} [0-9]{2}:[0-9]{2}:[0-9]{2} [0-9]{4}'; then
                        creation_date=$(echo "$creation_time_raw" | awk '{print $4 "-" $1 "-" $2 " " $3}' | cut -d':' -f1,2)
                    # Format: "Mmm DD HH:MM:SS" (no year)
                    elif echo "$creation_time_raw" | grep -qE '[A-Za-z]{3} [0-9]{1,2} [0-9]{2}:[0-9]{2}:[0-9]{2}'; then
                        creation_date=$(echo "$current_year-$creation_time_raw" | awk '{print $1 "-" $2 "-" $3 " " $4}' | cut -d':' -f1,2)
                    fi
                fi
            fi
            
            # Format usage percentage
            if [ -n "$usage" ] && [ "$usage" != "100.00" ]; then
                usage="${usage}%"
            else
                usage="N/A"
            fi
            
            # Print snapshot info in table format
            printf "| %-13s | %-10s | %-7s | %-5s | %-20s |\n" "$snapshot_name" "disk-$disk_num" "$lv_size" "$usage" "$creation_date"
        done
    done
    
    echo "========================================================================"
    
    # Check if we found any snapshots
    if ! lvs --noheadings 2>/dev/null | grep -E "vm-${vmid}-disk|vm--${vmid}--disk" | grep -q "snapshot"; then
        echo "No snapshots found for VM $vmid"
    fi
}

# check "LV snapshot status" match
check_snapshot_status() {
    local diskpath="$1"
    lvdisplay "$diskpath" | grep -q "^  LV snapshot status"
}

# check snapshot status merging
vm_disk_check_snapshot_merge() {
    local diskpath="$1"
    
    echo -n "Waiting for snapshot merging on $diskpath ... "
    while check_snapshot_status "$diskpath"; do
        echo -n "."
        sleep 10
    done

    echo
    echo " Snapshot merged on $diskpath."
}

# list vm disks, except local: type
vm_list_disks() {
    local vmid="$1"
    
    # parameters check
    if [ -z "$vmid" ]; then
        echo "Usage: vm_list_disks <vmid>"
        return 1
    fi

    # use qm config command to get vm config and filter lignes containing disks then exclude lignes containing "local:"
    local disks=$(qm config "$vmid" | grep -E '^virtio[0-9]+:|^sata[0-9]+:|^scsi[0-9]+:|^ide[0-9]+:' | grep -v 'local:')

    # parse each ligne containing disk and add lvm path
    echo "$disks" | while IFS= read -r line; do
        # extract disk name (field after ':')
        local disk_name=$(echo "$line" | cut -d ' ' -f 2 | cut -d ',' -f 1)
        
        # extract part after ':' in disk_name
        local disk_basename=$(echo "$disk_name" | cut -d ':' -f 2)    
        
        # get source path (real target) of symbolic link
        # use of find to get source path of symbolic link
        local lvm_path=$(find /dev -name "$disk_basename" -type l -exec ls -f {} +)

        if [ -n "$lvm_path" ]; then
            echo "$line,$disk_basename,$lvm_path"
        else
            echo "$line,$disk_basename,"
        fi
    done
}

# create snapshot for each vm disk
# create snapshot for each vm disk with improved space efficiency
vm_snapshot_create() {
    local vmid="$1"
    local snapshotname="$2"
    
    # parameters check
    if [ -z "$vmid" ] || [ -z "$snapshotname" ]; then
        echo "ERROR: Both vmid and snapshotname must be provided."
        return 1
    fi
    
    echo "Creating snapshot '$snapshotname' for VM $vmid..."
    
    # get list of vm disks
    local disks=$(vm_list_disks "$vmid")
    
    # Check if we found any disks
    if [ -z "$disks" ]; then
        echo "ERROR: No disks found for VM $vmid"
        return 1
    fi
    
    # Get disk usage stats for the VM to make better size estimates
    local total_success=true
    
    # Parse each disk line
    echo "$disks" | while IFS= read -r line; do
        # Extract device path (last element after comma)
        local device_path=$(echo "$line" | awk -F ',' '{print $NF}')
        
        # Skip if no valid device path
        if [ -z "$device_path" ] || [ ! -e "$device_path" ]; then
            echo "WARNING: Invalid device path found in disk list"
            continue
        fi
        
        # Get the LV info
        local vg_name=$(lvs "$device_path" --noheadings -o vg_name 2>/dev/null | tr -d ' ')
        local lv_name=$(lvs "$device_path" --noheadings -o lv_name 2>/dev/null | tr -d ' ')
        
        if [ -z "$vg_name" ] || [ -z "$lv_name" ]; then
            echo "WARNING: Could not determine VG/LV for $device_path"
            continue
        fi
        
        # Get disk size in a format LVM can use
        local disk_size=$(lvs "$device_path" --noheadings -o lv_size --units g 2>/dev/null | tr -d ' ')
        
        # Calculate intelligent snapshot size based on disk usage and type
        local snapshot_size=""
        
        # First try to get actual disk usage if VM is not running (more accurate)
        if ! qm status $vmid | grep -q "status: running"; then
            # Try to get filesystem usage (only works for mounted filesystems)
            # We mount the LV temporarily to check usage
            local mount_point=$(mktemp -d)
            
            if mount "$device_path" "$mount_point" 2>/dev/null; then
                # Get used space in GB
                local used_space=$(df -BG "$mount_point" | tail -1 | awk '{print $3}' | sed 's/G//')
                local total_space=$(df -BG "$mount_point" | tail -1 | awk '{print $2}' | sed 's/G//')
                
                # Calculate 20% more than used space, but minimum 10% of disk size
                local size_in_gb=$(echo "$disk_size" | sed 's/g//')
                local min_size=$(echo "$size_in_gb * 0.1" | bc | awk '{printf "%.0f", $1}')
                local calculated_size=$(echo "$used_space * 1.2" | bc | awk '{printf "%.0f", $1}')
                
                # Take the larger of the two values
                if [ "$calculated_size" -gt "$min_size" ]; then
                    snapshot_size="${calculated_size}G"
                else
                    snapshot_size="${min_size}G"
                fi
                
                # Unmount
                umount "$mount_point"
            fi
            rmdir "$mount_point" 2>/dev/null
        fi
        
        # If we couldn't determine usage, use a smarter default
        if [ -z "$snapshot_size" ]; then
            # Extract disk size from VM config
            local config_size=$(echo "$line" | grep -oP 'size=\K[^,]+')
            
            if [ -n "$config_size" ]; then
                # If it's a small disk, use 25% of size, if medium use 20%, if large use 15%
                local size_num=$(echo "$config_size" | grep -o '[0-9]\+')
                
                if [ "$size_num" -lt 50 ]; then
                    # Small disk (< 50G): 25%
                    snapshot_size=$(echo "$size_num * 0.25" | bc | awk '{printf "%.0f", $1}')
                elif [ "$size_num" -lt 200 ]; then
                    # Medium disk (50-200G): 20%
                    snapshot_size=$(echo "$size_num * 0.20" | bc | awk '{printf "%.0f", $1}')
                else
                    # Large disk (> 200G): 15%
                    snapshot_size=$(echo "$size_num * 0.15" | bc | awk '{printf "%.0f", $1}')
                fi
                
                # Add unit
                local size_unit=$(echo "$config_size" | grep -o '[A-Za-z]\+')
                if [ -z "$size_unit" ]; then
                    size_unit="G"  # Default to G if no unit specified
                fi
                snapshot_size="${snapshot_size}${size_unit}"
            else
                # Fallback: use 15% of actual disk size
                local size_in_gb=$(echo "$disk_size" | sed 's/g//')
                snapshot_size=$(echo "$size_in_gb * 0.15" | bc | awk '{printf "%.0f", $1}')
                snapshot_size="${snapshot_size}G"
            fi
        fi
        
        echo "Creating snapshot for disk: $device_path (Initial size: $snapshot_size)"
        
        # Execute snapshot creation
        if ! vm_disk_snapshot_create "$device_path" "$snapshotname" "$snapshot_size"; then
            total_success=false
        fi
    done
    
    if [ "$total_success" = "true" ]; then
        echo -e "\nSnapshot '$snapshotname' created successfully for VM $vmid"
        echo "NOTE: LVM will automatically extend snapshots if they reach 70% capacity"
    else
        echo -e "\nWARNING: Some snapshots may not have been created correctly"
    fi
    
    return 0
}

# snapshot delete for each vm disk
vm_snapshot_delete() {
    
    local vmid="$1"
    local snapshotname="$2"
    
    # parameters check
    if [ -z "$vmid" ] || [ -z "$snapshotname" ]; then
        echo "ERROR : Both vmid and snapshotname must be provided."
        return 1
    fi
    
    # get list of vm disks
    local disks=$(vm_list_disks "$vmid")
    
    # parse each disk line
    echo "$disks" | while IFS= read -r line; do

        # extract device path (last element after comma)
        local device_path=$(echo "$line" | awk -F ',' '{print $NF}')

        
        # call function to delete disk snapshot
        vm_disk_snapshot_delete "$device_path" "$snapshotname"
    done
    
    return 0
}

# revert to a vm snapshot
vm_revert_to_snapshot() {
    local vmid="$1"
    local snapshotname="$2"
    local autostart="$3"

    # parameters check
    if [ -z "$vmid" ] || [ -z "$snapshotname" ]; then
        echo "Usage: vm_revert_to_snapshot <vmid> <snapshotname>"
        return 1
    fi

    # check if vm exists
    if ! qm status $vmid >/dev/null 2>&1; then
        echo "ERROR: VM $vmid does not exist"
        return 1
    fi

    # check if vm is running and store initial state
    local vm_status=$(qm status $vmid)
    local vm_was_running=false
    
    if [ "$vm_status" = "status: running" ]; then
        vm_was_running=true
    fi

    # Ask about keeping snapshot
    local keep_snapshot=false
    read -p "Do you want to keep the snapshot after reverting? (y/n): " answer
    if [ "$answer" = "y" ] || [ "$answer" = "Y" ]; then
        keep_snapshot=true
        echo "Snapshot will be preserved after reverting."
    fi

    # If VM isn't running, start it for the revert process
    if [ "$vm_status" != "status: running" ]; then
        echo "Starting VM $vmid temporarily for reverting..."
        qm start $vmid
        
        # Wait for VM to fully start
        local wait_count=0
        while [ "$wait_count" -lt 30 ]; do
            if qm status $vmid | grep -q "status: running"; then
                break
            fi
            echo -n "."
            sleep 2
            wait_count=$((wait_count + 1))
        done
        
        if ! qm status $vmid | grep -q "status: running"; then
            echo "ERROR: Failed to start VM $vmid"
            return 1
        fi
        
        echo "VM started successfully."
    fi

    # ask confirmation to shutdown vm
    read -p "VM $vmid will be stopped during the revert process. Continue? (y/n): " answer
    if [ "$answer" != "y" ]; then
        echo "Task canceled."
        
        # If we started the VM but user cancels, stop it again
        if [ "$vm_status" != "status: running" ]; then
            echo "Stopping temporarily started VM..."
            qm stop $vmid
        fi
        
        return 1
    fi
        
    # get vm disk list before shutdown
    local disks=$(vm_list_disks "$vmid")
    
    echo "DEBUG::vm_revert_to_snapshot $1 $2 "
    
    # Create temp file for snapshots to recreate
    rm -f "/tmp/snapshots_to_recreate.$$" 2>/dev/null
    
    # parse each disk line for reverting
    local revert_success=true
    echo "$disks" | while IFS= read -r line; do
        # extract device path (last element after comma)
        local device_path=$(echo "$line" | awk -F ',' '{print $NF}')
        
        # check if device path is valid
        if [ -z "$device_path" ] || [ ! -e "$device_path" ]; then
            echo "WARNING: Invalid device path: $device_path"
            revert_success=false
            continue
        fi
        
        # call function to revert disk to snapshot
        if ! vm_disk_revert_to_snapshot "$device_path" "$snapshotname" "$keep_snapshot"; then
            revert_success=false
        fi
    done

    # Stop VM to complete the revert process
    echo "Stopping VM $vmid to complete snapshot revert..."
    qm stop $vmid
    
    # Wait for VM to stop
    local wait_count=0
    while [ "$wait_count" -lt 30 ]; do
        if ! qm status $vmid | grep -q "status: running"; then
            break
        fi
        echo -n "."
        sleep 2
        wait_count=$((wait_count + 1))
    done
    
    if qm status $vmid | grep -q "status: running"; then
        echo "WARNING: VM did not stop gracefully, forcing stop..."
        qm stop $vmid --skiplock
        sleep 5
    fi

    # Process each disk for LV activation
    echo "$disks" | while IFS= read -r line; do
        # extract device path (last element after comma)
        local device_path=$(echo "$line" | awk -F ',' '{print $NF}')
        
        # Skip invalid paths
        if [ -z "$device_path" ] || [ ! -e "$device_path" ]; then
            continue
        fi
        
        # call function to disable lv
        vm_disk_lv_desactivate "$device_path"
        
        sleep 1
        
        # call function to enable lv
        vm_disk_lv_activate "$device_path"
        
        # check and wait for end of snapshot merge
        vm_disk_check_snapshot_merge "$device_path"
    done
    
    # Recreate snapshots if requested
    if [ "$keep_snapshot" = "true" ]; then
        recreate_snapshots
    fi

    # Decide whether to restart the VM
    if [ "$autostart" = "true" ] || [ "$vm_was_running" = "true" ]; then
        echo "Starting VM $vmid..."
        qm start $vmid
        echo "VM $vmid started successfully."
    else
        echo "VM $vmid remains stopped. Use 'qm start $vmid' to start it when ready."
    fi

    return 0
}

# Clean up function
cleanup() {
    rm -f "/tmp/snapshots_to_recreate.$$" 2>/dev/null
}

# Set up trap to clean up on exit
trap cleanup EXIT INT TERM

# parameters get
action="$1"
vmid="$2"
snapshotname="$3"
autostart="true"  # Default: start VM after revert

# Check for --no-autostart option
for arg in "$@"; do
    if [ "$arg" = "--no-autostart" ]; then
        autostart="false"
    fi
done

# parameters check
if [ -z "$action" ] || [ -z "$vmid" ]; then
    echo "ERROR : action and vmid must be specified."
    usage
    exit 1
fi

# Switch parameter
case "$action" in
    create)
        if [ -z "$snapshotname" ]; then
            echo "ERROR : snapshotname parameter required for create action."
            usage
            exit 1
        fi
        vm_snapshot_create "$vmid" "$snapshotname"
        ;;
    delete)
        if [ -z "$snapshotname" ]; then
            echo "ERROR : snapshotname parameter required for delete action."
            usage
            exit 1
        fi
        vm_snapshot_delete "$vmid" "$snapshotname"
        ;;
    revert)
        if [ -z "$snapshotname" ]; then
            echo "ERROR : snapshotname parameter required for revert action"
            usage
            exit 1
        fi
        vm_revert_to_snapshot "$vmid" "$snapshotname" "$autostart"
        ;;
    list)
        vm_snapshot_list "$vmid"
        ;;
    *)
        echo "Action unkown : $action"
        usage
        exit 1
        ;;
esac
