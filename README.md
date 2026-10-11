# How To Use Coop-BT

This is a Behavior Tree Framework for multi-robot cooperation. It is based on the BT library project (https://fzi-forschungszentrum-informatik.github.io/ros2_ros_bt_py/index.html), meanwhile adds parts of nodes and code structures for heterogenous robot cooperation.
---


It includes a ReactJS-based web GUI and all the building blocks you need to build moderately advanced mission control Behavior Trees without writing a single line of code!

## Documentation

The main documentation effort is found in the `doc`folder.
Simply execute the following commands in your shell to get browsable HTML documentation, including some tutorials:

```bash
$ cd ros_bt_py/doc
$ make html
$ cd build
$ python -m http.server & xdg-open http://localhost:8000/html
```

## Installation

To actually start using ros_bt_py, you need to install its dependencies first:

```bash
$ cd colcon_workspace
$ rosdep install --from-paths src --ignore-src -r -y
```

Then you can just build the package with your prefered method i.e. `colcon build`

**Note**: Do not build the workspace using `--symlink-install` as this is currently not working
with generate_parameter_library. You will get an error when running `ModuleNotFoundError: No module named 'ros_bt_py.parameters'`.

## Running

The command
```bash
$ ros2 launch ros_bt_py ros_bt_py.launch.py enable_web_interface:=True
```

will start a BT server and the rosbridge and webserver needed for the
GUI.
Afterwards, you can open `http://localhost:8085/index.html` to use the editor.

### Launch Options

| **Launch Argument**            | **Description**                                                                             | **Default Value**                             |
|--------------------------------|---------------------------------------------------------------------------------------------|-----------------------------------------------|
| robot_namespace                | Namespace to launch all ros_bt_py nodes in!                                                 | /                                             |
| node_modules                   | List of python packages that contain nodes to be loaded on startup.                         | _"['ros_bt_py.nodes','ros_bt_py.ros_nodes']"_ |
| enable_web_interface           | Start web GUI on startup.                                                                   | _False_                                       |
| show_traceback_on_exception    | Show error traceback when an exception rises.                                               | _True_                                        |
| diagnostics_frequency_hz       | Publishing frequency for diagnostics msgs.                                                  | _1.0_                                         |
| load_default_tree              | Load the default tree on startup!                                                           | _False_                                       |
| load_default_tree_permissive   | Load the default tree in permissive mode on startup!                                        | _False_                                       |
| default_tree_path              | Path to the default tree to load on startup!                                                |                                               |
| default_tree_tick_frequency_hz | Frequency with which to tick the default tree loaded on startup!                            | _10.0_                                        |
| default_tree_control_command   | Command to execute per default after loading the default tree on startup!                   | _2_                                           |
| web_server_port                | Port to use for the web interface.                                                          | _8085_                                        |
| web_server_address             | IP address to use for the web interface. _Default value uses all IP addresses of the host._ | _0.0.0.0_                                     |

### Stand-alone Web Interface

The web interface can be launched stand alone of the library, using the following command:

```bash
$ ros2 launch ros_bt_py_web_gui ros_bt_py_web_gui.launch.py web_server_port:=8085 web_server_address:=0.0.0.0
```
# Coop-BT
