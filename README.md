# mtanvil

A Python library for parsing and editing Luanti worlds.

The name comes from Luanti’s former name (‘MT’ for Minetest) and the Minecraft world parsing library ‘anvil’

> This is extremely under development so please don't use this for any major projects right now. Future updates _will_ have breaking changes.
>
> However, testing is welcome so please do open an issue if you find problems with it.

mtanvil fully supports MapBlock format version 29 (latest). Other versions may not be fully supported but should receive full support in the future.

It is recommended that you familiarize yourself with the [Map File Format and MapBlock Serialization Format](https://github.com/luanti-org/luanti/blob/master/doc/world_format.md#map-file-format) so that you fully understand what data mtanvil provides.

## Usage

First of all, import mtanvil with

`import mtanvil as anvil`

You can then load a world file:

`world = anvil.world_from_file('/path/to/world/file/map.sqlite')`

### What to do with a world

* `list_mapblocks(world)`: Returns a list of all MapBlocks present in the world file

* `get_mapblock(world, pos)`: Returns the data of a MapBlock. `pos` should be a tuple of the XYZ coords, eg (5, -4, 18)

* `set_mapblock(world, pos, data)`: Sets the data of a MapBlock. `data` should be serialized and compressed, see data functions below

* `get_all_mapblocks(world)`: Returns a list of the data of all MapBlocks present in the world file. Each list item is a tuple: (X, Y, Z, data)

### What to do with data

* `parse_mapblock_data(data)`: Returns a dictionary of the parsed data. This "parsed data" is required for nearly every function below

* `set_node(data, pos, param0, param1=0, param2=0)`: Sets the node at the specified co-ordinates. `param0` is the node name, eg `default:goldblock`. Make sure you are providing the correct MapBlock, see utility functions below

* `serialize_mapblock_data(data)`: Turns parsed data back into binary. This must still be compressed before writing to a MapBlock, see functions below

### Utility functions

* `compress_mapblock_data(data)`: Compresses serialized data with zstd, as per the MapBlock format version 29 specifications

* `pos_get_mapblock(pos)`: Returns a tuplet with the MapBlock that has the world co-ordinates provided

* `pos_get_node(pos)`: Returns a tuplet with the position within the relevant MapBlock (see function above) of the world co-ordinates provided