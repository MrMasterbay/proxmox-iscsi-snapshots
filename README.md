
# Enhanced Proxmox LVM Snapshot Manager

A comprehensive snapshot management solution for Proxmox VE that supports both virtual machines and LXC containers with advanced cluster capabilities and intelligent LVM handling.

---

## 🚀 Features

### Core Functionality
- **Universal Support**: Complete snapshot management for both QEMU VMs and LXC containers
- **Intelligent Auto-Detection**: Automatic detection between VMs and containers using 6 different methods
- **Advanced LVM Support**:
  - Thin LVM snapshots (automatic sizing)
  - Regular LVM snapshots (intelligent size calculation)
  - Automatic thin/thick detection
- **Cluster Operations**: Full multi-node Proxmox cluster support with SSH automation

### Snapshot Operations
- **Create Snapshots**: Intelligent snapshot creation with automatic size calculation
- **Delete Snapshots**: Safe snapshot removal with verification
- **List Snapshots**: Detailed overview with creation dates, sizes, and types
- **Revert Snapshots**: Advanced revert functionality with snapshot preservation options

### Enhanced Capabilities
- **Snapshot Preservation**: Option to keep original snapshots during revert operations
- **Interactive & Non-Interactive Modes**: Full automation support for scripting
- **Cross-Node Execution**: Automatic detection and execution on correct cluster nodes
- **SSH Key Management**: Automated SSH setup for cluster communication
- **Comprehensive Error Handling**: Detailed diagnostics and troubleshooting information

---

## 📋 Prerequisites

### Required Configuration (on all PVE nodes)
1. Edit `/etc/lvm/lvm.conf` and ensure the following settings are enabled:
    ```text
    snapshot_autoextend_threshold = 70
    snapshot_autoextend_percent = 20
    ```

2. For cluster support:
   - SSH key authentication between cluster nodes
   - Same script deployed on all nodes

3. Ensure all required Proxmox CLI tools are available (`qm`, `pct`, `lvs`, `vgs`, `lvcreate`, etc.)

---

## 🛠️ Usage

### General Syntax
```bash
./snapshot.sh <action> <vmid/ctid> [<snapshotname>] [options]

```

### Actions

#### List Snapshots

```
./snapshot.sh list <vmid/ctid>

```

#### Create Snapshots

```
./snapshot.sh create <vmid/ctid> <snapshotname>

```

#### Delete Snapshots

```
./snapshot.sh delete <vmid/ctid> <snapshotname>

```

#### Revert Snapshots

```
./snapshot.sh revert <vmid/ctid> <snapshotname> [options]

```

### Command Line Options

#### Detection Options

-   `--container`: Force container mode (use pct commands)
-   `--vm`: Force VM mode (use qm commands)
-   Auto-detection is used if not specified

#### Revert Options

-   `--no-autostart`: Do not start VM/CT after revert (starts by default)
-   `--keep-snapshot`: Keep snapshot after revert (default: ask user)
-   `--delete-snapshot`: Delete snapshot after revert
-   `--non-interactive`: Skip all prompts (for automated execution)
-   `--interactive`: Force interactive mode even for remote execution

#### Cluster Options

-   `--force-local`: Force local-only operation (skip cluster coordination)
-   `--cluster-sync`: Force cluster synchronization even for local VMs/CTs
-   `--setup-ssh`: Setup SSH keys for cluster communication

#### Debug Options

-   `--debug`: Enable debug output for troubleshooting

* * * * *

📖 Examples
-----------

### Basic Operation

```
# List snapshots for VM 104
./snapshot.sh list 104

# List snapshots for container 108
./snapshot.sh list 108

# Create snapshot for VM (auto-detected)
./snapshot.sh create 104 backup-2025

# Create snapshot for container (auto-detected)
./snapshot.sh create 108 pre-update

# Delete snapshot
./snapshot.sh delete 104 backup-2025

# Revert with snapshot preservation
./snapshot.sh revert 104 backup-2025 --keep-snapshot

```

### Advanced Usage

```
# Force container mode if auto-detection fails
./snapshot.sh create 108 backup --container

# Non-interactive revert for automation
./snapshot.sh revert 104 backup --non-interactive --delete-snapshot --no-autostart

# Setup SSH keys for cluster
./snapshot.sh --setup-ssh

# Debug mode for troubleshooting
./snapshot.sh create 104 test --debug

```

### Cluster Operations

```
# Works automatically across cluster nodes
./snapshot.sh create 104 backup  # Executes on correct node automatically

# Force local execution only
./snapshot.sh create 104 backup --force-local

```

* * * * *

🔧 Advanced Features
--------------------

### Intelligent Snapshot Sizing

The script automatically calculates optimal snapshot sizes:

-   **Small disks** (<50G): 25% of disk size
-   **Medium disks** (50-200G): 20% of disk size
-   **Large disks** (>200G): 15% of disk size
-   **Thin LVM**: Automatic allocation as needed

### Cluster Integration

-   **Automatic node detection**: Finds where VMs/containers are located
-   **Remote execution**: Deploys and executes on correct nodes
-   **SSH automation**: Automatic SSH key setup and testing
-   **Fallback mechanisms**: Multiple methods for reliable operation

### Enhanced Detection System

The script uses 6 different methods to detect VM vs Container:

1.  Config file checking (`/etc/pve/lxc/` vs `/etc/pve/qemu-server/`)
2.  Status command testing (`pct status` vs `qm status`)
3.  Local list checking (`pct list` vs `qm list`)
4.  Cluster API integration (`pvesh`)
5.  Remote node checking via SSH
6.  LVM volume pattern analysis

* * * * *

🐛 Troubleshooting
------------------

### Debug Mode

Enable debug output for detailed troubleshooting:

bash

```
./snapshot.sh create 104 test --debug

```

### Detection Issues

If auto-detection fails, the script provides detailed diagnostics:

```
./snapshot.sh list 999  # Shows detection details for non-existent ID

```

### SSH Issues

Setup SSH keys for cluster communication:

```
./snapshot.sh --setup-ssh

```

* * * * *

🔄 Changelog
------------

### Major Enhancements Added

-   ✅ **Complete LXC Container Support**
-   ✅ **Enhanced Auto-Detection System** (6 methods)
-   ✅ **Advanced LVM Support** (thin/thick)
-   ✅ **Robust Cluster Support**
-   ✅ **Improved Remote Execution**
-   ✅ **Enhanced Command Line Options**
-   ✅ **Better Error Handling & Debugging**
-   ✅ **Intelligent Snapshot Sizing**
-   ✅ **Advanced Revert Functionality**
-   ✅ **Enhanced Snapshot Listing**

### New Functions

-   Enhanced detection with multiple fallback methods
-   Diagnostic information display
-   Thin LVM detection and handling
-   Snapshot preservation during revert
-   SSH automation and testing
-   Cross-cluster execution

* * * * *

🗺️ Roadmap
-----------

### 🎯 Version 2.0 - GUI Integration (Q2 2025)

-   **Native Proxmox VE Integration**

    -   Proxmox VE plugin development
    -   Integration with existing Proxmox web interface
    -   Custom menu items and panels
    -   Seamless authentication with Proxmox users/permissions
-   **Enhanced GUI Features**

    -   Visual snapshot timeline and management
    -   Drag-and-drop snapshot operations
    -   Bulk operations interface
    -   Advanced filtering and search capabilities


🤝 Contributing
---------------

We welcome contributions! Feel free to:

-   Submit **pull requests** for improvements
-   Report **issues** via GitHub Issues
-   Suggest **new features** or enhancements
-   Improve **documentation**

### Development

The script is designed to be:

-   **Cluster-ready** for production environments
-   **Automation-friendly** with non-interactive modes
-   **User-friendly** with comprehensive error messages
-   **Extensible** for future enhancements

* * * * *

📄 License
----------

This program is free software: you can redistribute it and/or modify it under the terms of the **GNU Affero General Public License** as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but **WITHOUT ANY WARRANTY**; without even the implied warranty of **MERCHANTABILITY** or **FITNESS FOR A PARTICULAR PURPOSE**. See the **GNU Affero General Public License** for more details.

You should have received a copy of the **GNU Affero General Public License** along with this program. If not, see <https://www.gnu.org/licenses/agpl-3.0.html>.

* * * * *

📞 Support
----------

For questions, support, or feature requests:

-   🐛 **Issues**: [GitHub Issues](https://github.com/MrMasterbay/proxmox-iscsi-snapshots/issues)
-   💬 **Discussions**: [Discord Coming Soon]
-   📧 **Contact**: <nico.schmidt@ns-tech.cloud>

### Follow Development

-   🔗 **Links**: [linktr.ee/bagstube_nico](https://linktr.ee/bagstube_nico)
-   💖 **Support**: [ko-fi.com/bagstube_nico](https://ko-fi.com/bagstube_nico)

* * * * *

**Enhanced Proxmox LVM Snapshot Manager** - Making snapshot management simple, reliable, and powerful! 🚀
