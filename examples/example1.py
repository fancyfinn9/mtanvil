# This example opens a world, loads the MapBlock that contains node 0,0,0, sets it to a gold block, and writes it

import mtanvil as anvil

world = anvil.World.from_file('/path/to/map.sqlite')

mapblock = world.get_mapblock((0,0,0))

mapblock.set_node((0,0,0), "default:goldblock").set_node((0,1,0), "default:goldblock") # It chains!

world.set_mapblock((0,0,0), mapblock.serialize())