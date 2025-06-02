# VM Snapshot Management Script

This script provides enhanced functionality for managing snapshots of VMs on Proxmox VE (PVE) nodes. It simplifies common snapshot operations for virtual machines, including creating, deleting, listing, and reverting snapshots. Additionally, it includes advanced features like preserving snapshots after reverting and better snapshot management with timestamps.

---

## Features

### Current Features
- **Create Snapshots**: Create snapshots for all VM disks intelligently, with calculated sizes based on disk usage.
- **Delete Snapshots**: Remove snapshots for all VM disks associated with a specific VM.
- **Revert to Snapshots**: Safely revert VMs to a saved snapshot with options to:
  - Preserve snapshots after reverting.
  - Avoid automatic VM startup after reverting.
- **List Snapshots**: View an overview of snapshots for a VM, including:
  - Snapshot Name
  - Disk Information
  - Size
  - Usage
  - Creation Date (with timestamp formatting).
- **Logical Volume Management**: Includes commands to activate/deactivate logical volumes (LVs) and handle snapshot merging.

### Enhancements
- Improved snapshot capability with better size calculations.
- Snapshot overview with timestamps for easier identification.
- Option to preserve snapshots after reverting.
- "No Autostart" option to allow manual VM startup after reverting.

### Planned Enhancements
- **Proxmox GUI Support**: Integration with the Proxmox VE web interface for managing snapshots directly through the GUI.

---

## Prerequisites

### Required Configuration (on all PVE nodes)
1. Edit `/etc/lvm/lvm.conf` and ensure the following settings are enabled:
    ```text
    snapshot_autoextend_threshold = 70
    snapshot_autoextend_percent = 20
    ```

2. Ensure all required Proxmox CLI tools (`qm`, `lvs`, `vgs`, `lvcreate`, etc.) are available and properly configured.

---

## Usage

### General Syntax
```sh
./snapshot.sh <action> <vmid> [<snapshotname>] [options]
 ```

### Actions
Lists all snapshots for a VM.
```sh
./snapshot.sh list <vmid>
```
Creates a snapshot for a VM
```sh
./snapshot.sh create <vmid> <snapshotname>
```

Deletes a specific snapshot for a VM
```sh
./snapshot.sh delete <vmid> <snapshotname>
```

Reverts a VM to a specific snapshot
```sh
./snapshot.sh revert <vmid> <snapshotname> [--no-autostart]
```

Options
--no-autostart: Prevents the VM from starting automatically after the revert action. The default behavior is to start the VM.

### Example Commands

1. List all snapshots for VM 101:
```sh
./snapshot.sh list 101
```

2. Create a snapshot named "backup-2025" for VM 101:
```sh
./snapshot.sh create 101 backup-2025
```

3. Delete a snapshot named "backup-2025" for VM 101:
```sh
./snapshot.sh delete 101 backup-2025
```

4. Revert VM 101 to the "backup-2025" snapshot without starting the VM automatically:
```sh
./snapshot.sh revert 101 backup-2025 --no-autostart
```

---

## Advanced Usage

### Intelligent Snapshot Sizing
The script calculates snapshot sizes intelligently based on:
- **Disk usage** (if the VM is not running).
- **Configured disk size**, with a percentage-based allocation for efficient storage utilization.

### Snapshot Merging
After reverting to a snapshot, the script:
- Automatically handles **snapshot merging**.
- **Recreates preserved snapshots** when required, ensuring data integrity.

---

## Debugging and Logs

- Debug outputs are prefixed with `DEBUG::` for easier identification during script execution.
- Temporary files used for snapshot recreation are stored under `/tmp/` and are **automatically cleaned up** after the script finishes.

---

## Contributions

### Added Improvements
- **Better snapshot capability** with intelligent sizing.
- **Enhanced snapshot overview** with clear timestamps.
- **"Keep Snapshot" feature** after reverting snapshots.
- **"No Autostart" option** for reverting VM snapshots, allowing manual control.

### Planned Features
- **Official Proxmox GUI support**: Enabling snapshot management directly through the Proxmox web interface.

### Contributions Welcome
We welcome contributions to this script! Feel free to:
- Submit **pull requests** for improvements.
- Suggest **new features** or report issues via the GitHub repository's Issues section.

---

## License

This program is free software: you can redistribute it and/or modify it under the terms of the **GNU Affero General Public License** as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
This program is distributed in the hope that it will be useful, but **WITHOUT ANY WARRANTY**; without even the implied warranty of **MERCHANTABILITY** or **FITNESS FOR A PARTICULAR PURPOSE**. See the **GNU Affero General Public License** for more details.
You should have received a copy of the **GNU Affero General Public License** along with this program. If not, see [https://www.gnu.org/licenses/agpl-3.0.html].

---

For questions or support, please reach out via the **GitHub repository's Issues section**.
