import sqlite3
import zstandard as zstd
import zlib
import struct
import io

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

type_to_format = {
    "u8": ">B",
    "s8": ">b",
    "u16": ">H",
    "s16": ">h",
    "u32": ">I",
    "s32": ">i",
    "u64": ">Q",
    "s64": ">q",
    "f32": ">f",
    "f64": ">d"
}

def unpack(type_name, data):
    if not type_name in type_to_format:
        raise ValueError("Invalid format")
    return struct.unpack(type_to_format[type_name], data)[0]

def pack(type_name, data):
    if not type_name in type_to_format:
        raise ValueError("Invalid format")
    return struct.pack(type_to_format[type_name], data)

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

def is_inventory(data):
    data = io.BytesIO(data)
    data.readline() # Initial data
    
    while True:
        line = data.readline().split(b" ")
        if line[0] == b"EndInventory\n" or line[0] == b"end":
            return True
        elif line[0] == b"Width":
            pass
        elif line[0] == b"Item":
            pass
        elif line[0] == b"Empty\n":
            pass
        elif line[0] == b"EndInventoryList\n":
            pass
        else:
            return False

def extract_inventory(data):
    pos = data.find(b'EndInventory\n')
    if pos > -1:
        inventory, data = pop_bytes(data, pos+len(b'EndInventory\n'))
        return inventory, data
    return None, data

class Node:
    def __init__(self, data=None):
        self.pos = None
        self.raw = data
        self.data = data or {"name": "ignore", "param1": 0, "param2": 0, "metadata": [], "timers": []}

    def set_name(self, name):
        self.data["name"] = name

    def set_param1(self, param1):
        self.data["param1"] = param1

    def set_param2(self, param2):
        self.data["param2"] = param2

class StaticObject:
    def __init__(self, object_type, pos, data):
        self.object_type = object_type
        self.pos = pos
        self.raw = data
        self.data = self.parse(data)

    def parse(self, data=None):
        if data is None:
            data = self.raw
            if not data:
                return None

        parsed_data = {
            "compatibility_byte": None, "entity_name_length": None, "entity_name": None,
            "static_data_length": None, "static_data": None,
            "hp": None, "velocity": None, "yaw": None,
            "version2": None, "pitch": None, "roll": None, "guid": None
        }

        parsed_data["compatibility_byte"], data = pop_bytes(data, 1)
        if unpack("u8", parsed_data["compatibility_byte"]) != 1:
            print("WARNING: compatibility_byte is not 1")

        parsed_data["entity_name_length"], data = pop_bytes(data, 2)

        parsed_data["entity_name"], data = pop_bytes(data, unpack("u16", parsed_data["entity_name_length"]))

        parsed_data["static_data_length"], data = pop_bytes(data, 4)

        parsed_data["static_data"], data = pop_bytes(data, unpack("u32", parsed_data["static_data_length"]))

        parsed_data["hp"], data = pop_bytes(data, 2)

        velocity_x, data = pop_bytes(data, 4)
        velocity_y, data = pop_bytes(data, 4)
        velocity_z, data = pop_bytes(data, 4)

        parsed_data["velocity"] = (velocity_x, velocity_y, velocity_z)

        parsed_data["yaw"], data = pop_bytes(data, 4)

        if len(data) > 0: # Since protocol version 37
            parsed_data["version2"], data = pop_bytes(data, 1)
            if not (unpack("u8", parsed_data["version2"]) > 0 and unpack("u8", parsed_data["version2"]) < 3):
                print("WARNING: version2 is not 1 or 2")

            parsed_data["pitch"], data = pop_bytes(data, 4)

            parsed_data["roll"], data = pop_bytes(data, 4)

            if unpack("u8", parsed_data["version2"]) >= 2:
                parsed_data["guid"], data = pop_bytes(data, 16)

        pretty_data = {
            "compatibility_byte": None, "entity_name": None, "static_data": None,
            "hp": None, "velocity": None, "yaw": None,
            "version2": None, "pitch": None, "roll": None, "guid": None
        }

        pretty_data["compatibility_byte"] = unpack("u8", parsed_data["compatibility_byte"])

        pretty_data["entity_name"] = parsed_data["entity_name"].decode("utf-8")

        pretty_data["static_data"] = parsed_data["static_data"].decode("utf-8")

        pretty_data["hp"] = unpack("s16", parsed_data["hp"])

        velocity_x = unpack("s32", parsed_data["velocity"][0])/10000
        velocity_y = unpack("s32", parsed_data["velocity"][1])/10000
        velocity_z = unpack("s32", parsed_data["velocity"][2])/10000
        pretty_data["velocity"] = (velocity_x, velocity_y, velocity_z)

        pretty_data["yaw"] = unpack("s32", parsed_data["yaw"])/1000

        if parsed_data["version2"]:
            pretty_data["version2"] = unpack("u8", parsed_data["version2"])

            pretty_data["pitch"] = unpack("s32", parsed_data["pitch"])/1000

            pretty_data["roll"] = unpack("s32", parsed_data["roll"])/1000

            if pretty_data["version2"] >= 2:
                pretty_data["guid"] = parsed_data["guid"]

        return pretty_data

    def serialize(self, data=None):
        if data is None:
            data = self.data
            if data is None:
                return None

        serialized_data = bytearray()

        # u8 compatibility_byte
        serialized_data.extend(pack("u8", (data["compatibility_byte"] or 1)))

        # u16 len
        serialized_data.extend(pack("u16", len(data["entity_name"].encode("utf-8"))))

        # u8[len] entity name
        serialized_data.extend(data["entity_name"].encode("utf-8"))

        # u32 len
        serialized_data.extend(pack("u32", len(data["static_data"].encode("utf-8"))))

        # u8[len] static data
        serialized_data.extend(data["static_data"].encode("utf-8"))

        # s16 hp
        serialized_data.extend(pack("s16", data["hp"]))

        # s32 velocity.x * 10000
        serialized_data.extend(pack("s32", int(data["velocity"][0]*10000)))

        # s32 velocity.y * 10000
        serialized_data.extend(pack("s32", int(data["velocity"][1]*10000)))

        # s32 velocity.z * 10000
        serialized_data.extend(pack("s32", int(data["velocity"][2]*10000)))

        # s32 yaw * 1000
        serialized_data.extend(pack("s32", int(data["yaw"]*1000)))

        # Since protocol version 37:

        if data["version2"]:
            # u8 version2 (=1 or 2)
            serialized_data.extend(pack("u8", data["version2"]))

            # s32 pitch * 1000
            serialized_data.extend(pack("s32", int(data["pitch"]*1000)))

            # s32 roll * 1000
            serialized_data.extend(pack("s32", int(data["roll"]*1000)))

            # if version2 >= 2:
            if data["version2"] >= 2:
                # u8[16] guid
                serialized_data.extend(data["guid"])

        serialized_data = bytes(serialized_data)

        return serialized_data

class MapBlock:
    def __init__(self, pos=None, data=None, verbose=True):
        self.pos = pos
        self.raw = data
        self.data = self.parse(data, verbose=verbose) or {
            "was_compressed": None,
            "version": 29, "flags": {"is_underground": False, "day_night_differs": True, "lighting_expired": True, "generated": False},
            "lighting_complete": {"nothing1": True, "nothing2": True, "nothing3": True, "nothing4": True,
                "night": {"X-": False, "Y-": False, "Z-": False, "Z+": False, "Y+": False, "X+": False},
                "day": {"X-": False, "Y-": False, "Z-": False, "Z+": False, "Y+": False, "X+": False}},
            "timestamp": 4294967295,
            "name_id_mapping_version": 0, "name_id_mappings": [],
            "content_width": 2, "params_width": 2, "node_data": [], "nodes": [],
            "node_metadata_version": 2, "node_metadata": [],
            "static_object_version": 0, "static_objects": [],
            "length_of_single_timer": 10, "timers": []
        }
        
    def parse(self, data=None, verbose=True):
        if data is None:
            return None
        
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
        version = unpack("u8", parsed_data["version"])

        if version >= 29: # Map format version 29+ compresses the entire MapBlock data (excluding the version byte) with zstd
            try:
                data = zstd_decompress(data)
                parsed_data["was_compressed"] = True
            except zstd.backend_c.ZstdError as e:
                #print("> zstd error: "+str(e))
                if verbose:
                    print("Could not decompress MapBlock data! Attempting to parse the raw data...")
                parsed_data["was_compressed"] = False
        
        parsed_data["flags"], data = pop_bytes(data, 1)

        if version >= 27:
            parsed_data["lighting_complete"], data = pop_bytes(data, 2)

        if version >= 29:
            parsed_data["timestamp"], data = pop_bytes(data, 4)

            parsed_data["name_id_mapping_version"], data = pop_bytes(data, 1) # Should be 0 (map format version 29 (current))
            if struct.unpack(">B", parsed_data["name_id_mapping_version"])[0] != 0 and verbose:
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
        content_width = struct.unpack(">B", parsed_data["content_width"])[0]
        if version < 24 and content_width != 1 and verbose:
            print("WARNING: content_width is not 1")
        elif version >= 24 and content_width != 2 and verbose:
            print("WARNING: content_width is not 2")

        parsed_data["params_width"], data = pop_bytes(data, 1) # Should be 2
        params_width = struct.unpack(">B", parsed_data["params_width"])[0]
        if params_width != 2 and verbose:
            print("WARNING: params_width is not 2")

        # Node data (+ node metadata) is Zlib-compressed before map version format 29
        # TODO: find the end of the compressed section so that we can decompress it

        param0_fields = []
        param1_fields = []
        param2_fields = []
        
        for _ in range(4096): # param0: Either 1 byte x 4096 or 2 bytes x 4096
            param0, data = pop_bytes(data, content_width)
            param0_fields.append(param0)

        for _ in range(4096): # param1: 1 byte x 4096
            param1, data = pop_bytes(data, params_width // 2)
            param1_fields.append(param1)

        for _ in range(4096): # param2: 1 byte x 4096
            param2, data = pop_bytes(data, params_width // 2)
            param2_fields.append(param2)
        
        node_data = []
        for n in range(len(param0_fields)):
            node = {"param0": param0_fields[n], "param1": param1_fields[n], "param2": param2_fields[n]}
            node_data.append(node)
        parsed_data["node_data"] = node_data

        if version < 23:
            parsed_data["node_metadata_version"], data = pop_bytes(data, 2)
            if struct.unpack(">H", parsed_data["node_metadata_version"])[0] != 1 and verbose:
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

        elif version >= 23:
            parsed_data["node_metadata_version"], data = pop_bytes(data, 1)
            if struct.unpack(">B", parsed_data["node_metadata_version"])[0] == 0 and verbose:
                print("INFO: node_metadata_version is 0, skipping node metadata")
            elif version < 28 and struct.unpack(">B", parsed_data["node_metadata_version"])[0] != 1 and verbose:
                print("WARNING: node_metadata_version is not 1")
            elif version >= 28 and struct.unpack(">B", parsed_data["node_metadata_version"])[0] != 2 and verbose:
                print("WARNING: node_metadata_version is not 2")

            if struct.unpack(">B", parsed_data["node_metadata_version"])[0] != 0: 
                parsed_data["num_node_metadata"], data = pop_bytes(data, 2)

                all_metadata = []
                for _ in range(struct.unpack(">H", parsed_data["num_node_metadata"])[0]):
                    metadata = {"position": None, "num_vars": None, "vars": None}

                    metadata["position"], data = pop_bytes(data, 2)

                    metadata["num_vars"], data = pop_bytes(data, 4)

                    var_s = []
                    for _ in range(unpack("u32", metadata["num_vars"])):
                        var = {"key_len": None, "key": None, "val_len": None, "value": None, "is_private": None}

                        var["key_len"], data = pop_bytes(data, 2)

                        var["key"], data = pop_bytes(data, struct.unpack(">H", var["key_len"])[0])

                        var["val_len"], data = pop_bytes(data, 4)

                        if var["key"].decode("utf-8") == "infotext" and is_inventory(data): # This is the most reliable way to check if this is an inventory
                            var["value"], data = extract_inventory(data)

                        else:
                            var["value"], data = pop_bytes(data, unpack("u32", var["val_len"]))

                        if struct.unpack(">B", parsed_data["node_metadata_version"])[0] == 2:
                            var["is_private"], data = pop_bytes(data, 1)
                            if struct.unpack(">B", var["is_private"])[0] != 0 and struct.unpack(">B", var["is_private"])[0] != 1 and verbose:
                                print("WARNING: metadata's is_private is not 0 or 1, metadata may be corrupted")

                        var_s.append(var)
                    
                    if len(var_s) > 0:
                        metadata["vars"] = var_s

                    all_metadata.append(metadata)

                if len(all_metadata) > 0:
                    parsed_data["node_metadata"] = all_metadata

        # TODO: implement Map format version 23 + 24 node timers

        # Static objects (node timers were moved to after this in map format version 25+)

        parsed_data["static_object_version"], data = pop_bytes(data, 1)
        if struct.unpack(">B", parsed_data["static_object_version"])[0] != 0 and verbose:
            print("WARNING: static_object_version is not 0")

        parsed_data["static_object_count"], data = pop_bytes(data, 2)

        static_objects = []
        for _ in range(struct.unpack(">H", parsed_data["static_object_count"])[0]):
            static_object = {"type": None, "pos_x": None, "pos_y": None, "pos_z": None, "data_size": None, "data": None}

            static_object["type"], data = pop_bytes(data, 1)

            static_object["pos_x"], data = pop_bytes(data, 4)

            static_object["pos_y"], data = pop_bytes(data, 4)

            static_object["pos_z"], data = pop_bytes(data, 4)

            static_object["data_size"], data = pop_bytes(data, 2)

            static_object["data"], data = pop_bytes(data, struct.unpack(">H", static_object["data_size"])[0])

            # TODO: parse data further

            static_objects.append(static_object)

        if len(static_objects) > 0:
            parsed_data["static_objects"] = static_objects

        # Timestamp + Name ID Mappings (map format version >29)

        if version < 29:
            parsed_data["timestamp"], data = pop_bytes(data, 4)

            parsed_data["name_id_mapping_version"], data = pop_bytes(data, 1) # Should be 0
            if struct.unpack(">B", parsed_data["name_id_mapping_version"])[0] != 0 and verbose:
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

        if version >= 25:
            parsed_data["length_of_single_timer"], data = pop_bytes(data, 1) # Should be 10 (2+4+4)
            if struct.unpack(">B", parsed_data["length_of_single_timer"])[0] != 10 and verbose:
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

        pretty_data = {
            "was_compressed": None,
            "version": None, "flags": {"is_underground": None, "day_night_differs": None, "lighting_expired": None, "generated": None},
            "lighting_complete": {"nothing1": None, "nothing2": None, "nothing3": None, "nothing4": None,
                "night": {"X-": None, "Y-": None, "Z-": None, "Z+": None, "Y+": None, "X+": None},
                "day": {"X-": None, "Y-": None, "Z-": None, "Z+": None, "Y+": None, "X+": None}},
            "timestamp": None,
            "name_id_mapping_version": None, "name_id_mappings": [],
            "content_width": None, "params_width": None, "node_data": [], "nodes": [],
            "node_metadata_version": None, "node_metadata": [],
            "static_object_version": None, "static_objects": [],
            "length_of_single_timer": None, "timers": []
        }

        pretty_data["was_compressed"] = parsed_data["was_compressed"]
        pretty_data["version"] = version

        flags_int = unpack("u8", parsed_data["flags"])
        pretty_data["flags"]["is_underground"] = bool(flags_int & 0x01)
        pretty_data["flags"]["day_night_differs"] = bool(flags_int & 0x02)
        pretty_data["flags"]["lighting_expired"] = bool(flags_int & 0x04)
        pretty_data["flags"]["generated"] = bool(flags_int & 0x08)

        # TODO: find a way to do this with less code: bool(parsed_data["lighting_complete"] & (1 << X)) is used every time

        if parsed_data["lighting_complete"] is not None:
            lighting_int = unpack("u16", parsed_data["lighting_complete"])
            pretty_data["lighting_complete"]["nothing1"] = bool(lighting_int & (1 << 15))
            pretty_data["lighting_complete"]["nothing2"] = bool(lighting_int & (1 << 14))
            pretty_data["lighting_complete"]["nothing3"] = bool(lighting_int & (1 << 13))
            pretty_data["lighting_complete"]["nothing4"] = bool(lighting_int & (1 << 12))
            pretty_data["lighting_complete"]["night"]["X-"] = bool(lighting_int & (1 << 11))
            pretty_data["lighting_complete"]["night"]["Y-"] = bool(lighting_int & (1 << 10))
            pretty_data["lighting_complete"]["night"]["Z-"] = bool(lighting_int & (1 << 9))
            pretty_data["lighting_complete"]["night"]["Z+"] = bool(lighting_int & (1 << 8))
            pretty_data["lighting_complete"]["night"]["Y+"] = bool(lighting_int & (1 << 7))
            pretty_data["lighting_complete"]["night"]["X+"] = bool(lighting_int & (1 << 6))
            pretty_data["lighting_complete"]["day"]["X-"] = bool(lighting_int & (1 << 5))
            pretty_data["lighting_complete"]["day"]["Y-"] = bool(lighting_int & (1 << 4))
            pretty_data["lighting_complete"]["day"]["Z-"] = bool(lighting_int & (1 << 3))
            pretty_data["lighting_complete"]["day"]["Z+"] = bool(lighting_int & (1 << 2))
            pretty_data["lighting_complete"]["day"]["Y+"] = bool(lighting_int & (1 << 1))
            pretty_data["lighting_complete"]["day"]["X+"] = bool(lighting_int & (1 << 0))

        pretty_data["timestamp"] = unpack("u32", parsed_data["timestamp"])

        pretty_data["name_id_mapping_version"] = unpack("u8", parsed_data["name_id_mapping_version"])

        for mapping in (parsed_data["name_id_mappings"] or []):
            pretty_data["name_id_mappings"].append({"id": unpack("u16", mapping["id"]), "name": mapping["name"].decode("utf-8")})
        
        pretty_data["content_width"] = unpack("u8", parsed_data["content_width"])
        pretty_data["params_width"] = unpack("u8", parsed_data["params_width"])

        # TODO: add safeguards to make sure content_width and params_width are valid

        for node in parsed_data["node_data"]:
            pretty_data["node_data"].append({"param0": unpack("u"+str(pretty_data["content_width"]*8), node["param0"]), "param1": unpack("u"+str(pretty_data["params_width"]*4), node["param1"]), "param2": unpack("u"+str(pretty_data["params_width"]*4), node["param2"])})

        if version < 23:
            pretty_data["node_metadata_version"] = unpack("u16", parsed_data["node_metadata_version"])

        else:
            pretty_data["node_metadata_version"] = unpack("u8", parsed_data["node_metadata_version"])
            
            for metadata in (parsed_data["node_metadata"] or []):
                pretty_metadata = {"position": unpack("u16", metadata["position"]), "vars": []}

                for var in (metadata["vars"] or []):
                    is_private = False
                    if var.get("is_private"):
                        is_private = bool(unpack("u8", var["is_private"]) & 0x01)
                    pretty_metadata["vars"].append({"key": var["key"].decode("utf-8"), "value": var["value"].decode("utf-8"), "is_private": is_private})

                pretty_data["node_metadata"].append(pretty_metadata)

        pretty_data["static_object_version"] = unpack("u8", parsed_data["static_object_version"])

        for static_object in (parsed_data["static_objects"] or []):
            pretty_data["static_objects"].append(
                StaticObject(
                    unpack("u8", static_object["type"]),
                    (
                        unpack("s32", static_object["pos_x"])/10000,
                        unpack("s32", static_object["pos_y"])/10000,
                        unpack("s32", static_object["pos_z"])/10000
                    ),
                    static_object["data"]
                )
            )

        if parsed_data["length_of_single_timer"] is not None:
            pretty_data["length_of_single_timer"] = unpack("u8", parsed_data["length_of_single_timer"])

        for timer in (parsed_data["timers"] or []):
            pretty_data["timers"].append({"position": unpack("u16", timer["position"]), "timeout": unpack("s32", timer["timeout"])/1000, "elapsed": unpack("s32", timer["elapsed"])/1000})

        new_nodes = []
        for node in pretty_data["node_data"]:
            name = ""
            for mapping in pretty_data["name_id_mappings"]:
                if mapping["id"] == node["param0"]:
                    name = mapping["name"]
                    break
            new_nodes.append({"name": name, "param1": node["param1"], "param2": node["param2"], "metadata": [], "timers": []})
        for metadata in pretty_data["node_metadata"]:
            new_nodes[metadata["position"]]["metadata"] = metadata["vars"]
        for timer in pretty_data["timers"]:
            new_nodes[timer["position"]]["timers"].append({"timeout": timer["timeout"], "elapsed": timer["elapsed"]})
        
        node_classes = []
        for node in new_nodes:
            node_classes.append(Node(node))

        pretty_data["nodes"] = node_classes

        return pretty_data

    def serialize(self, data=None, compressed=True):
        if data == None:
            data = self.data

        serialized_data = bytearray()

        # TODO: support serializing in other MapBlock format versions?

        if data["version"] != 29:
            print("WARNING: data will be converted to MapBlock format version 29")

        # Quickly ensure that all of the node pos's are correct
        
        node_pos = 0
        for node in data["nodes"]:
            node.pos = node_pos
            node_pos += 1

        # u8 version
        serialized_data.extend(pack("u8", 29))

        # u8 flags
        if data["flags"]:
            flags = 0
            if data["flags"]["is_underground"]:
                flags |= 0x01
            if data["flags"]["day_night_differs"]:
                flags |= 0x02
            if data["flags"]["lighting_expired"]:
                flags |= 0x04
            if data["flags"]["generated"]:
                flags |= 0x08

            serialized_data.extend(pack("u8", flags))
        else:
            flags = 0
            flags &= ~0x01 # is_underground
            flags |= 0x02 # day_night_differs
            flags |= 0x04 # lighting_expired (deprecated)
            flags |= 0x08 # generated
            serialized_data.extend(pack("u8", flags))

        # u16 lighting_complete
        if data["lighting_complete"]:
            lighting_complete = 0
            if data["lighting_complete"]["nothing1"]:
                lighting_complete |= (1 << 15)
            if data["lighting_complete"]["nothing2"]:
                lighting_complete |= (1 << 14)
            if data["lighting_complete"]["nothing3"]:
                lighting_complete |= (1 << 13)
            if data["lighting_complete"]["nothing4"]:
                lighting_complete |= (1 << 12)
            if data["lighting_complete"]["night"]["X-"]:
                lighting_complete |= (1 << 11)
            if data["lighting_complete"]["night"]["Y-"]:
                lighting_complete |= (1 << 10)
            if data["lighting_complete"]["night"]["Z-"]:
                lighting_complete |= (1 << 9)
            if data["lighting_complete"]["night"]["Z+"]:
                lighting_complete |= (1 << 8)
            if data["lighting_complete"]["night"]["Y+"]:
                lighting_complete |= (1 << 7)
            if data["lighting_complete"]["night"]["X+"]:
                lighting_complete |= (1 << 6)
            if data["lighting_complete"]["day"]["X-"]:
                lighting_complete |= (1 << 5)
            if data["lighting_complete"]["day"]["Y-"]:
                lighting_complete |= (1 << 4)
            if data["lighting_complete"]["day"]["Z-"]:
                lighting_complete |= (1 << 3)
            if data["lighting_complete"]["day"]["Z+"]:
                lighting_complete |= (1 << 2)
            if data["lighting_complete"]["day"]["Y+"]:
                lighting_complete |= (1 << 1)
            if data["lighting_complete"]["day"]["X+"]:
                lighting_complete |= (1 << 0)

            serialized_data.extend(pack("u16", lighting_complete))
        else:
            lighting_complete = 0b1111111111111110
            serialized_data.extend(pack("u16", lighting_complete))

        # u32 timestamp
        if data["timestamp"]:
            serialized_data.extend(pack("u32", data["timestamp"]))
        else:
            timestamp = 0xffffffff # Invalid/unknown timestamp
            serialized_data.extend(pack("u32", timestamp))

        # u8 name_id_mapping_version
        serialized_data.extend(pack("u8", 0)) # Should be 0

        names = []
        for node in data["nodes"]:
            if not node.data["name"] in names:
                names.append(node.data["name"])

        name_id_mappings = {}
        current_id = 0
        for name in names:
            name_id_mappings[name] = current_id
            current_id += 1

        # u16 num_name_id_mappings
        if name_id_mappings:
            serialized_data.extend(pack("u16", len(name_id_mappings)))

            # foreach num_name_id_mappings

            for name, mapping_id in name_id_mappings.items():
                # u16 id
                serialized_data.extend(pack("u16", mapping_id))

                # u16 name_len
                serialized_data.extend(pack("u16", len(name.encode("utf-8"))))

                # u8[name_len] name
                serialized_data.extend(name.encode("utf-8"))
        else:
            serialized_data.extend(pack("u16", 0))

        # u8 content_width
        serialized_data.extend(pack("u8", (data["content_width"] or 2))) # Should be 2

        # u8 params_width
        serialized_data.extend(pack("u8", (data["params_width"] or 2))) # Should be 2

        # u<content_width*8>[4096] param0 fields
        for node in data["nodes"]:
            serialized_data.extend(pack("u"+str((data["content_width"] or 2)*8), name_id_mappings[node.data["name"]]))

        # u8[4096] param1 fields
        for node in data["nodes"]:
            serialized_data.extend(pack("u"+str((data["params_width"] or 2)*4), node.data["param1"]))

        # u8[4096] param2 fields
        for node in data["nodes"]:
            serialized_data.extend(pack("u"+str((data["params_width"] or 2)*4), node.data["param2"]))

        # u8 node_metadata_version
        # If there is 0 node metadata, this is 0, otherwise it is 2
        node_metadata = []
        for node in data["nodes"]:
            if node.data["metadata"]:
                node_metadata.append(node)

        if len(node_metadata) > 0:
            serialized_data.extend(pack("u8", 2))

            # u16 num_node_metadata
            serialized_data.extend(pack("u16", len(node_metadata)))

            # foreach num_node_metadata
            for node in node_metadata:
                # u16 position
                serialized_data.extend(pack("u16", node.pos))

                # u32 num_vars
                if node.data["metadata"]:
                    if len(node.data["metadata"]) > 0:
                        serialized_data.extend(pack("u32", len(node.data["metadata"])))
                        
                        # foreach num_vars
                        for var in node.data["metadata"]:
                            # u16 key_len
                            serialized_data.extend(pack("u16", len(var["key"])))

                            # u8[key_len] key
                            serialized_data.extend(var["key"].encode("utf-8"))

                            # u32 val_len
                            serialized_data.extend(pack("u32", len(var["value"])))

                            # u8[val_len] value
                            serialized_data.extend(var["value"].encode("utf-8"))

                            # u8 is_private
                            if var["is_private"]:
                                serialized_data.extend(pack("u8", 1))
                            else:
                                serialized_data.extend(pack("u8", 0))
                    else:
                        serialized_data.extend(pack("u32", 0))
                else:
                    serialized_data.extend(pack("u32", 0))
        else:
            serialized_data.extend(pack("u8", 0))

        # u8 static object version
        serialized_data.extend(pack("u8", (data["static_object_version"] or 0)))

        # u16 static_object_count
        if data["static_objects"]:
            if len(data["static_objects"]) > 0:
                serialized_data.extend(pack("u16", len(data["static_objects"])))

                # foreach static_object_count
                for obj in data["static_objects"]:
                    # u8 type
                    serialized_data.extend(pack("u8", obj.object_type))

                    # s32 pos_x_nodes * 10000
                    serialized_data.extend(pack("s32", int(obj.pos[0]*10000)))

                    # s32 pos_y_nodes * 10000
                    serialized_data.extend(pack("s32", int(obj.pos[1]*10000)))

                    # s32 pos_z_nodes * 10000
                    serialized_data.extend(pack("s32", int(obj.pos[2]*10000)))

                    serialized = obj.serialize()

                    if serialized:
                        # u16 data_size
                        serialized_data.extend(pack("u16", len(serialized)))

                        # u8[data_size] data
                        serialized_data.extend(serialized)
                    else:
                        # u16 data_size
                        serialized_data.extend(pack("u16", 0))
            else:
                serialized_data.extend(pack("u16", 0))
        else:
            serialized_data.extend(pack("u16", 0))
            
        # u8 length_of_single_timer
        serialized_data.extend(pack("u8", (data["length_of_single_timer"] or 10)))

        timers = []
        for node in data["nodes"]:
            if len(node.data["timers"]) > 0:
                for timer in node.data["timers"]:
                    timers.append({"position": node.pos, "timeout": timer["timeout"], "elapsed": timer["elapsed"]})

        # u16 num_of_timers
        if len(timers) > 0:
            serialized_data.extend(pack("u16", len(timers)))

            # foreach num_of_timers
            for timer in timers:
                # u16 timer_position
                serialized_data.extend(pack("u16", timer["position"]))

                # s32 timeout
                serialized_data.extend(pack("s32", int(timer["timeout"]*1000)))

                # s32 elapsed
                serialized_data.extend(pack("s32", int(timer["elapsed"]*1000)))

        else:
            serialized_data.extend(pack("u16", 0))

        serialized_data = bytes(serialized_data)

        if compressed:
            serialized_data = serialized_data[:1] + zstd_compress(serialized_data[1:])

        return serialized_data

    def get_node(self, posxyz):
        return self.data["nodes"][(posxyz[2]*16*16 + posxyz[1]*16 + posxyz[0])]

    def set_node(self, posxyz, node):
        pos = (posxyz[2]*16*16 + posxyz[1]*16 + posxyz[0])
        if pos < 0 or pos > 4095:
            return self
        if not isinstance(node, Node):
            return self

        node.pos = pos

        self.data["nodes"][pos] = node

        return self

class World:
    def __init__(self, conn):
        self.conn = conn
        self.filename = "<unknown>"

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    @classmethod
    def from_file(cls, filename):
        conn = sqlite3.connect(filename)
        instance = cls(conn)
        instance.filename = filename
        return instance

    def list_mapblocks(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT x, y, z FROM blocks")
        rows = cursor.fetchall()

        mapblocks = []
        for row in rows:
            mapblocks.append((row[0], row[1], row[2]))

        return mapblocks

    def get_mapblock(self, pos, verbose=True):
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT data FROM blocks WHERE x=? AND y=? AND z=?",
            (pos[0], pos[1], pos[2])
        )
        row = cursor.fetchone()
        if row:
            return MapBlock(pos=pos, data=row[0], verbose=verbose)
        return None

    def set_mapblock(self, pos, mapblock):
        if isinstance(mapblock, MapBlock):
            mapblock = mapblock.serialize()
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE blocks SET data=? WHERE x=? AND y=? AND z=?",
            (sqlite3.Binary(mapblock), pos[0], pos[1], pos[2])
        )
        self.conn.commit()

    def get_all_mapblocks(self):
        mapblocks = []
        for mapblock in self.list_mapblocks():
            mapblocks.append((mapblock[0], mapblock[1], mapblock[2], self.get_mapblock(mapblock)))

        return mapblocks
