# This example opens a world, loads the MapBlock that contains node 0,0,0, sets some nodes, and writes it

import mtanvil as anvil

world = anvil.World.from_file('/path/to/map.sqlite')

mapblock = world.get_mapblock((0,0,0))

# Option 1: Get an existing node
node = mapblock.get_node((0,0,0))

# Option 2: Create a blank node
node = anvil.Node()
node.set_name("default:goldblock")

for i in range(5):
    mapblock.set_node((0,i,0), node)

world.set_mapblock((0,0,0), mapblock)

world.close()