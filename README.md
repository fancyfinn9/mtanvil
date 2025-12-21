# mtanvil

A Python library for parsing and editing Luanti worlds.

The name comes from Luanti’s former name (‘MT’ for Minetest) and the Minecraft world parsing library ‘anvil’

> This is extremely under development so please don't use this for any major projects right now. Future updates _will_ have breaking changes.
>
> However, testing is welcome so please do open an issue if you find problems with it.

mtanvil fully supports MapBlock format version 29 (latest). Other versions may not be fully supported but should receive full support in the future.

It is recommended that you familiarize yourself with the [Map File Format and MapBlock Serialization Format](https://github.com/luanti-org/luanti/blob/master/doc/world_format.md#map-file-format) so that you fully understand what data mtanvil provides.

## Usage

> Currently mtanvil is not available as a package (yet), so you must download **mtanvil.py** and put it in the same directory as the file that will import it.

First of all, import mtanvil with

`import mtanvil as anvil`

You can then load a world file:

`world = anvil.World.from_file('/path/to/map.sqlite')`

### World functions

* `world.list_mapblocks()`: Returns a list of all MapBlocks present in the world file

* `world.get_mapblock(pos)`: Returns a MapBlock. `pos` should be a tuple of the XYZ coords, eg (5, -4, 18)

* `world.set_mapblock(pos, data)`: Writes a MapBlock to the world. `data` should be serialized, see MapBlock's functions

> NOTE: you should ensure that the world is currently not in use by Luanti before writing to it

* `world.get_all_mapblocks()`: Returns all MapBlocks present in the world file. Each list item is a tuple: (X, Y, Z, MapBlock)

* `world.close()`: Closes the database connection. It is recommended to run this once you're finished with the World

### MapBlock functions

* `mapblock.data`: Dictionary of the parsed data

* `mapblock.parse(data)`: Returns a dictionary of the parsed data. `data` must be a raw binary blob from the database

* `mapblock.set_node(pos, param0, param1 = 0, param2 = 0)`: Sets the node at the specified co-ordinates. `param0` is the node name, eg `default:goldblock`

* `mapblock.serialize(data = None, compressed = True)`: Turns dictionary of parsed data back into binary. If `data` is `None`, `mapblock.data` will be serialized instead. `compressed` is required for Luanti MapBlock format version 29+

### Utility functions

* `pos_get_mapblock(pos)`: Returns a tuplet with the position of the MapBlock that has the world co-ordinates provided

* `pos_get_node(pos)`: Returns a tuplet with the position within the relevant MapBlock (see function above) of the world co-ordinates provided