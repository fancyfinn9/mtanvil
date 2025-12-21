import sqlite3
import zstandard as zstd
import zlib
import struct
import io

def world_from_file(file):
    conn = sqlite3.connect(file)
    return conn

def pop_bytes(data, n):
    if len(data) < n:
        raise ValueError(f"Need {n} bytes, have {len(data)}")
    return data[:n], data[n:]

def zstd_decompress(data):
    decompressor = zstd.ZstdDecompressor()
    with decompressor.stream_reader(io.BytesIO(data)) as reader:
        data = reader.read()
    return data

def zstd_compress(data):
    compressor = zstd.ZstdCompressor()
    data = compressor.compress(data)
    return data

def compress_mapblock_data(data):
    return data[:1] + zstd_compress(data[1:])
    

def parse_mapblock_data(data):
    parsed_data = {
        "was_compressed": None,
        "version": None, "flags": None, "lighting_complete": None, "timestamp": None,
        "name_id_mapping_version": None, "num_name_id_mappings": None, "name_id_mappings": None,
        "content_width": None, "params_width": None, "node_data": None,
        "node_metadata_version": None, "num_node_metadata": None, "node_metadata": None,
        "static_object_version": None, "static_object_count": None, "static_objects": None,
        "length_of_single_timer": None, "num_of_timers": None, "timers": None
    }

    parsed_data["version"], data = pop_bytes(data, 1)

    if struct.unpack(">B", parsed_data["version"])[0] >= 29: # Map format version 29+ compresses the entire MapBlock data (excluding the version byte) with zstd
        try:
            data = zstd_decompress(data)
            parsed_data["was_compressed"] = True
        except zstd.backend_c.ZstdError as e:
            #print("> zstd error: "+str(e))
            print("Could not decompress MapBlock data! Attempting to parse the raw data...")
            parsed_data["was_compressed"] = False
    
    parsed_data["flags"], data = pop_bytes(data, 1)

    if struct.unpack(">B", parsed_data["version"])[0] >= 27:
        parsed_data["lighting_complete"], data = pop_bytes(data, 2)

    if struct.unpack(">B", parsed_data["version"])[0] >= 29:
        parsed_data["timestamp"], data = pop_bytes(data, 4)

        parsed_data["name_id_mapping_version"], data = pop_bytes(data, 1) # Should be 0 (map format version 29 (current))
        if struct.unpack(">B", parsed_data["name_id_mapping_version"])[0] != 0:
            print("WARNING: name_id_mapping_version is not 0")

        parsed_data["num_name_id_mappings"], data = pop_bytes(data, 2)

        mappings = []
        for _ in range(struct.unpack(">H", parsed_data["num_name_id_mappings"])[0]):
            mapping = {"id": None, "name_len": None, "name": None}

            mapping["id"], data = pop_bytes(data, 2)

            mapping["name_len"], data = pop_bytes(data, 2)

            mapping["name"], data = pop_bytes(data, struct.unpack(">H", mapping["name_len"])[0])

            mappings.append(mapping)

        if len(mappings) > 0:
            parsed_data["name_id_mappings"] = mappings
    
    parsed_data["content_width"], data = pop_bytes(data, 1) # Should be 2 (map format version 24+) or 1
    if struct.unpack(">B", parsed_data["version"])[0] < 24 and struct.unpack(">B", parsed_data["content_width"])[0] != 1:
        print("WARNING: content_width is not 1")
    elif struct.unpack(">B", parsed_data["version"])[0] >= 24 and struct.unpack(">B", parsed_data["content_width"])[0] != 2:
        print("WARNING: content_width is not 2")

    parsed_data["params_width"], data = pop_bytes(data, 1) # Should be 2
    if struct.unpack(">B", parsed_data["params_width"])[0] != 2:
        print("WARNING: params_width is not 2")

    # Node data (+ node metadata) is Zlib-compressed before map version format 29
    # TODO: find the end of the compressed section so that we can decompress it

    param0_fields = []
    param1_fields = []
    param2_fields = []
    
    for _ in range(4096): # param0: Either 1 byte x 4096 or 2 bytes x 4096
        param0, data = pop_bytes(data, struct.unpack(">B", parsed_data["content_width"])[0])
        param0_fields.append(param0)

    for _ in range(4096): # param1: 1 byte x 4096
        param1, data = pop_bytes(data, 1)
        param1_fields.append(param1)

    for _ in range(4096): # param2: 1 byte x 4096
        param2, data = pop_bytes(data, 1)
        param2_fields.append(param2)
    
    node_data = []
    for n in range(len(param0_fields)):
        node = {"param0": param0_fields[n], "param1": param1_fields[n], "param2": param2_fields[n]}
        node_data.append(node)
    parsed_data["node_data"] = node_data

    if struct.unpack(">B", parsed_data["version"])[0] < 23:
        parsed_data["node_metadata_version"], data = pop_bytes(data, 2)
        if struct.unpack(">H", parsed_data["node_metadata_version"])[0] != 1:
            print("WARNING: node_metadata_version is not 1")
        
        parsed_data["num_node_metadata"], data = pop_bytes(data, 2)

        all_metadata = []
        for _ in range(struct.unpack(">H", parsed_data["num_node_metadata"])[0]):
            metadata = {"position": None, "type_id": None, "content_size": None, "content": None}

            metadata["position"], data = pop_bytes(data, 2)

            metadata["type_id"], data = pop_bytes(data, 2)

            metadata["content_size"], data = pop_bytes(data, 2)

            metadata["content"], data = pop_bytes(data, struct.unpack(">H", metadata["content_size"])[0])
            
            # TODO: parse all the different type_id's

            all_metadata.append(metadata)

        parsed_data["node_metadata"] = all_metadata

    elif struct.unpack(">B", parsed_data["version"])[0] >= 23:
        parsed_data["node_metadata_version"], data = pop_bytes(data, 1)
        if struct.unpack(">B", parsed_data["node_metadata_version"])[0] == 0:
            print("WARNING: node_metadata_version is 0, skipping node metadata")
        elif struct.unpack(">B", parsed_data["version"])[0] < 28 and struct.unpack(">B", parsed_data["node_metadata_version"])[0] != 1:
            print("WARNING: node_metadata_version is not 1")
        elif struct.unpack(">B", parsed_data["version"])[0] >= 28 and struct.unpack(">B", parsed_data["node_metadata_version"])[0] != 2:
            print("WARNING: node_metadata_version is not 2")

        if struct.unpack(">B", parsed_data["node_metadata_version"])[0] != 0: 
            parsed_data["num_node_metadata"], data = pop_bytes(data, 2)

            all_metadata = []
            for _ in range(struct.unpack(">H", parsed_data["num_node_metadata"])[0]):
                metadata = {"position": None, "num_vars": None, "vars": None}

                metadata["position"], data = pop_bytes(data, 2)

                metadata["num_vars"], data = pop_bytes(data, 4)

                var_s = []
                for _ in range(struct.unpack(">I", metadata["num_vars"])[0]):
                    var = {"key_len": None, "key": None, "val_len": None, "value": None, "is_private": None}

                    var["key_len"], data = pop_bytes(data, 2)

                    var["key"], data = pop_bytes(data, struct.unpack(">H", var["key_len"])[0])

                    var["val_len"], data = pop_bytes(data, 2)

                    var["value"], data = pop_bytes(data, struct.unpack(">H", var["val_len"])[0])

                    if struct.unpack(">B", parsed_data["node_metadata_version"])[0] == 2:

                        var["is_private"], data = pop_bytes(data, 1)
                        if struct.unpack(">B", var["is_private"])[0] != 0 and struct.unpack(">B", var["is_private"])[0] != 1:
                            print("WARNING: metadata's is_private is not 0 or 1, metadata may be corrupted")
                    
                    var_s.append(var)
                
                if len(var_s) > 0:
                    metadata["vars"] = var_s

                # TODO: find out how serialized inventory is saved if it's empty, and implement serialized inventory

                all_metadata.append(metadata)

            if len(all_metadata) > 0:
                parsed_data["node_metadata"] = all_metadata

    # TODO: implement Map format version 23 + 24 node timers

    # Static objects (node timers were moved to after this in map format version 25+)

    parsed_data["static_object_version"], data = pop_bytes(data, 1)
    if struct.unpack(">B", parsed_data["static_object_version"])[0] != 0:
        print("WARNING: static_object_version is not 0")

    parsed_data["static_object_count"], data = pop_bytes(data, 2)

    static_objects = []
    for _ in range(struct.unpack(">H", parsed_data["static_object_count"])[0]):
        static_object = {"type": None, "pos_x_nodes": None, "pos_y_nodes": None, "pos_z_nodes": None, "data_size": None, "data": None}

        static_object["type"], data = pop_bytes(data, 1)

        static_object["pos_x_nodes"], data = pop_bytes(data, 4)

        static_object["pos_y_nodes"], data = pop_bytes(data, 4)

        static_object["pos_z_nodes"], data = pop_bytes(data, 4)

        static_object["data_size"], data = pop_bytes(data, 2)

        static_object["data"], data = pop_bytes(data, struct.unpack(">H", static_object["data_size"])[0])

        # TODO: parse data further

        static_objects.append(static_object)

    if len(static_objects) > 0:
        parsed_data["static_objects"] = static_objects

    # Timestamp + Name ID Mappings (map format version >29)

    if struct.unpack(">B", parsed_data["version"])[0] < 29:
        parsed_data["timestamp"], data = pop_bytes(data, 4)

        parsed_data["name_id_mapping_version"], data = pop_bytes(data, 1) # Should be 0
        if struct.unpack(">B", parsed_data["name_id_mapping_version"])[0] != 0:
            print("WARNING: name_id_mapping_version is not 0")

        parsed_data["num_name_id_mappings"], data = pop_bytes(data, 2)

        mappings = []
        for _ in range(struct.unpack(">H", parsed_data["num_name_id_mappings"])[0]):
            mapping = {"id": None, "name_len": None, "name": None}

            mapping["id"], data = pop_bytes(data, 2)

            mapping["name_len"], data = pop_bytes(data, 2)

            mapping["name"], data = pop_bytes(data, struct.unpack(">H", mapping["name_len"])[0])

            mappings.append(mapping)

        if len(mappings) > 0:
            parsed_data["name_id_mappings"] = mappings

    # Node Timers (map format version 25+)

    if struct.unpack(">B", parsed_data["version"])[0] >= 25:
        parsed_data["length_of_single_timer"], data = pop_bytes(data, 1) # Should be 10 (2+4+4)
        if struct.unpack(">B", parsed_data["length_of_single_timer"])[0] != 10:
            print("WARNING: length_of_single_timer is not 10")

        parsed_data["num_of_timers"], data = pop_bytes(data, 2)

        timers = []
        for _ in range(struct.unpack(">H", parsed_data["num_of_timers"])[0]):
            timer = {"position": None, "timeout": None, "elapsed": None}

            timer["position"], data = pop_bytes(data, 2)

            timer["timeout"], data = pop_bytes(data, 4)

            timer["elapsed"], data = pop_bytes(data, 4)

            timers.append(timer)

        if len(timers) > 0:
            parsed_data["timers"] = timers

    return parsed_data

def serialize_mapblock_data(data):
    serialized_data = bytearray()

    # TODO: support serializing in other MapBlock format versions?

    if struct.unpack(">B", data["version"])[0] != 29:
        print("WARNING: data will be converted to MapBlock format version 29")

    # u8 version
    serialized_data.extend(struct.pack(">B", 29))

    # u8 flags
    if data["flags"]:
        serialized_data.extend(data["flags"])
    else:
        flags = 0
        flags &= ~0x01 # is_underground
        flags |= 0x02 # day_night_differs
        flags |= 0x04 # lighting_expired (deprecated)
        flags |= 0x08 # generated
        serialized_data.extend(struct.pack(">B", flags))

    # u16 lighting_complete
    if data["lighting_complete"]:
        serialized_data.extend(data["lighting_complete"])
    else:
        lighting_complete = 0b1111111111111110
        serialized_data.extend(struct.pack(">H", lighting_complete))

    # u32 timestamp
    if data["timestamp"]:
        serialized_data.extend(data["timestamp"])
    else:
        timestamp = 0xffffffff # Invalid/unknown timestamp
        serialized_data.extend(struct.pack(">I", timestamp))

    # u8 name_id_mapping_version
    serialized_data.extend(struct.pack(">B", 0)) # Should be 0

    # u16 num_name_id_mappings
    if data["name_id_mappings"]:
        serialized_data.extend(struct.pack(">H", len(data["name_id_mappings"])))

        # foreach num_name_id_mappings

        for mapping in data["name_id_mappings"]:
            # u16 id
            serialized_data.extend(mapping["id"])

            # u16 name_len
            serialized_data.extend(struct.pack(">H", len(mapping["name"])))

            # u8[name_len] name
            serialized_data.extend(mapping["name"])
    else:
        serialized_data.extend(struct.pack(">H", 0))

    # u8 content_width
    serialized_data.extend(struct.pack(">B", 2)) # Should be 2

    # u8 params_width
    serialized_data.extend(struct.pack(">B", 2)) # Should be 2

    # u16[4096] param0 fields
    for node in data["node_data"]:
        serialized_data.extend(node["param0"])

    # u8[4096] param1 fields
    for node in data["node_data"]:
        serialized_data.extend(node["param1"])

    # u8[4096] param2 fields
    for node in data["node_data"]:
        serialized_data.extend(node["param2"])

    # u8 node_metadata_version
    # If there is 0 node metadata, this is 0, otherwise it is 2
    if data["node_metadata"]:
        if len(data["node_metadata"]) > 0:
            serialized_data.extend(struct.pack(">B", 2))

            # u16 num_node_metadata
            serialized_data.extend(struct.pack(">H", len(data["node_metadata"])))

            # foreach num_node_metadata
            for node in data["node_metadata"]:
                # u16 position
                serialized_data.extend(node["position"])

                # u32 num_vars
                if node["vars"]:
                    if len(node["vars"]) > 0:
                        serialized_data.extend(struct.pack(">I", len(node["vars"])))
                        
                        # foreach num_vars
                        for var in node["vars"]:
                            # u16 key_len
                            serialized_data.extend(struct.pack(">H", len(var["key"])))

                            # u8[key_len] key
                            serialized_data.extend(var["key"])

                            # u16 val_len
                            serialized_data.extend(struct.pack(">H", len(var["value"])))

                            # u8[val_len] value
                            serialized_data.extend(var["value"])

                            # u8 is_private
                            if struct.unpack(">B", var["is_private"]) == 1:
                                serialized_data.extend(struct.pack(">B", 1))
                            else:
                                serialized_data.extend(struct.pack(">B", 0))

                    else:
                        serialized_data.extend(struct.pack(">I", 0))
                else:
                    serialized_data.extend(struct.pack(">I", 0))


        else:
            serialized_data.extend(struct.pack(">B", 0))
    else:
        serialized_data.extend(struct.pack(">B", 0))

    # u8 static object version
    serialized_data.extend(struct.pack(">B", 0))

    # u16 static_object_count
    if data["static_objects"]:
        if len(data["static_objects"]) > 0:
            serialized_data.extend(struct.pack(">H", len(data["static_objects"])))

            # foreach static_object_count
            for obj in data["static_objects"]:
                # u8 type
                serialized_data.extend(obj["type"])

                # s32 pos_x_nodes * 10000
                serialized_data.extend(obj["pos_x_nodes"])

                # s32 pos_y_nodes * 10000
                serialized_data.extend(obj["pos_y_nodes"])

                # s32 pos_z_nodes * 10000
                serialized_data.extend(obj["pos_z_nodes"])

                # u16 data_size
                serialized_data.extend(struct.pack(">H", len(obj["data"])))

                # u8[data_size] data
                serialized_data.extend(obj["data"])
        else:
            serialized_data.extend(struct.pack(">H", 0))
    else:
        serialized_data.extend(struct.pack(">H", 0))
        
    # u8 length_of_single_timer
    serialized_data.extend(struct.pack(">B", 10))

    # u16 num_of_timers
    if data["timers"]:
        if len(data["timers"]) > 0:
            serialized_data.extend(struct.pack(">H", len(data["timers"])))

            # foreach num_of_timers
            for timer in data["timers"]:
                # u16 timer_position
                serialized_data.extend(timer["position"])

                # s32 timeout
                serialized_data.extend(timer["timeout"])

                # s32 elapsed
                serialized_data.extend(timer["elapsed"])

        else:
            serialized_data.extend(struct.pack(">H", 0))
    else:
        serialized_data.extend(struct.pack(">H", 0))

    return bytes(serialized_data)

def list_mapblocks(world):
    cursor = world.cursor()
    cursor.execute(f"SELECT * FROM blocks")
    rows = cursor.fetchall()

    mapblocks = []
    for row in rows:
        mapblocks.append((row[0], row[1], row[2]))

    return mapblocks

def get_mapblock(world, mapblock):
    cursor = world.cursor()
    cursor.execute(
        "SELECT data FROM blocks WHERE x=? AND y=? AND z=?",
        (mapblock[0], mapblock[1], mapblock[2])
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    return None

def set_mapblock(world, pos, blob):
    cursor = world.cursor()
    cursor.execute(
        "UPDATE blocks SET data=? WHERE x=? AND y=? AND z=?",
        (sqlite3.Binary(blob), pos[0], pos[1], pos[2])
    )
    world.commit()

def get_all_mapblocks(world):
    mapblocks = []
    for mapblock in list_mapblocks(world):
        mapblocks.append((mapblock[0], mapblock[1], mapblock[2], get_mapblock(world, mapblock)))

    return mapblocks

def pos_get_mapblock(pos):
    return (
        pos[0] // 16,
        pos[1] // 16,
        pos[2] // 16,
    )

def pos_get_node(pos):
    return (
        pos[0] % 16,
        pos[1] % 16,
        pos[2] % 16,
    )

def set_node(data, posxyz, param0, param1=0, param2=0):
    pos_mapblock = pos_get_mapblock(posxyz)
    pos_node = pos_get_node(posxyz)

    pos = (pos_node[2]*16*16 + pos_node[1]*16 + pos_node[0])

    mapping_id = None
    
    for mapping in data["name_id_mappings"]:
        if mapping["name"].decode("utf-8") == param0:
            mapping_id = mapping["id"]

    if not mapping_id:
        used_ids = []
        for mapping in data["name_id_mappings"]:
            used_ids.append(struct.unpack(">H", mapping["id"])[0])
        
        next_id = 0
        while next_id in used_ids:
            next_id += 1

        data["name_id_mappings"].append({"id": struct.pack(">H", next_id), "name_len": struct.pack(">H", len(param0.encode("utf-8"))), "name": param0.encode("utf-8")})
        mapping_id = struct.pack(">H", next_id)

    data["node_data"][pos]["param0"] = mapping_id
    data["node_data"][pos]["param1"] = struct.pack(">B", param1)
    data["node_data"][pos]["param2"] = struct.pack(">B", param2)

    return data

#print(parse_mapblock_data(b'\x1d(\xb5/\xfd\x00X\xcd\x14\x00V\xd0,#\xa0%\xc9p\xff\xff\xff\xff\xff\x8fP\xfb-\xbb-n\xc3ZG\xf0\xbf\xf7\xff\xc2]{\xef\xbd\xf7\xded\xef-e\n,\x00\x18\x00(\x00\x03\xa3\xfcH\x01\xc0\xc1\xe0\xe6\x87dp\xb4\xfbQ0\x0c\x03\t\x82 \x0c\x0c\x01\x97\t\x08\xc2H\n\x04\x0e\x84\x00\x06\x01r\xbc\xcd\xe7\xd1\x9fH4m\xc9[~[\xb2\xff\x93\xff$Y6\xff\xe4\x85\xfc{\xb9\x90m\xef\xbcM\x02\xc5y\xae1\xa5TJ\t\x15\xef\xbd\xb5\x96R*\xa1\xf7\xdec\x8c\xad\xb5TBo\xf7\xcd\xfd\xa8\xb5\xc6\x18[*\xa5\xa4\x16k\x8e\x01\xbf\xbe\xe4\xbc\xb4\xa5\xfd\xff\xdf\x18S\xca\xff\x17\xe7\x1cc\x1e\xa1\xa2(\x8es\xce}J\t!\x14\x81w\xa8\xa1\xde\xa0 \x05\x85\x1a\xed\x061\x07"$m\x1a\x03\x12 Q\x1c\x85c\x1c\x03!$\xc7 \x10B\x8c1\x86\x18C\x182\x8c\x88\x88\x04"C2\xd4\xd5\xf8\x07\x1aCJ7\xd1nAs{\xc0\xbd(}(=o\xa9\xeb\n\x9dx\x08\x03\xdc\xbfES\xae\xf6%ZCy\xee:\xf1\x81\xac\xb09\x11\x03\xbf\xfd\xc8^:\xf2\xed\x99\xf2\x19\r\r\xa4\xb8t)N!74Q\x0b\xe1\xb6\xef7\xbb&\x1f\xb2m\x96\x94\xed\xe40\xcbg/z\xd1\x85\x94\xf0\xbd\xae\\pQ\xe1Z\xdb\x96\xefO4B\x8dU\xe0\xacA\xadY\x83\x9f!b\xd8\xa3_\xb3\xbb\xd7\x82\xf5\x1bK`n\xfa\x97\xd7\xc9\xd1\x9d\x18A\t\xdf\xba\xa1\x89\xfb\xb1\xf2\x1dGX\x95\xd3Y\xc9\xf7\x9ds\xa0\xe9\x7f\xfc\xcf\xe6\x1a\xcc\xab\xf3\xc9\xd4E\x02\xd7\xf6\x99\t\x03\x9c\xb7y\xd1\x98\t\x8e\xe0{\xbd\x0f\x12\x18@\'\xf1+\xf9$4\xda\xe2\x8b?\xb7\xea\xfd\xfe\xf4\xb2-!\xb8G\x07\x1c\xab\xac\xb2#\xc8\xbe\xa7pFJ>\xb41\xbd^?\x02\xc4\x1b\xba\xc1+\xa9\x98N\xd9/\xfd\x10S^\x8a,\xf1\xb2^#Y\x1b\x98\x18,[9L\x04D\x99\x14/\xe7\x94\xd1\x7fF\xaf\xfc~\xc0;\x82cR\x07G\x8f\x91c\xf1~E\xe4W\xae\xabZ\x03\xd9\xe3\xb3\x00t\xe8\xfc+Ud\x0e\t\x80\xc0*N\xb2\xe2\xce`\xe3o\xb3\\W\xf6,\x01\n\xe6\x05"\xdeO\xef\xcc\x1dP\x0f\x84\xf0\xc54\xee\xfa\xed\xa0\xf94\xe2a\xf8\xef\x0e\x06\x03\xe6\x1dq\xc7\xa9>\xe3\x11\xea\xc9\xa5\x89s\xce\xce\xda;\xb5O)\x17\xa1\xff2.5\x9cTdqkz\xeax\x020\x84Az\x08\x8f\xbe\x1aP\xdc\x04\xc7\xcb\xd3\x1e\xf8\xc8\xcc\xe0\xd8\x16\xd7\xa7\x8epF"\x04\xa3\xd3\x907 \xaf1\x86\xdd\x02tl\xa4\x08\xaa\x0c\xfa]\xee3\xf5s\xc0\xf8\x1fQ\xaa<?\xca-U\x01'))