import mtanvil as anvil

world = anvil.world_from_file('/home/USERNAME/luanti/worlds/decompilingworld/map.sqlite')

data = anvil.parse_mapblock_data(anvil.get_mapblock(world, (0,0,0)))

anvil.set_node(data, (0,0,0), "default:goldblock")

anvil.set_mapblock(world, (0,0,0), anvil.compress_mapblock_data(anvil.serialize_mapblock_data(data)))