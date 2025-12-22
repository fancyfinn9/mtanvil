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
    return struct.unpack(integer_to_format[type_name], data)[0]

def pack(type_name, data):
    if not type_name in type_to_format:
        raise ValueError("Invalid format")
    return struct.pack(integer_to_format[type_name], data)

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

class MapBlock:
    def __init__(self, pos, data):
        self.pos = pos
        self.raw = data
        self.data = self.parse(data)
        
    def parse(self, data=None):
        if data is None:
            data = self.raw
        
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
                print("Could not decompress MapBlock data! Attempting to parse the raw data...")
                parsed_data["was_compressed"] = False
        
        parsed_data["flags"], data = pop_bytes(data, 1)

        if version >= 27:
            parsed_data["lighting_complete"], data = pop_bytes(data, 2)

        if version >= 29:
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
        if version < 24 and struct.unpack(">B", parsed_data["content_width"])[0] != 1:
            print("WARNING: content_width is not 1")
        elif version >= 24 and struct.unpack(">B", parsed_data["content_width"])[0] != 2:
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

        if version < 23:
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

        elif version >= 23:
            parsed_data["node_metadata_version"], data = pop_bytes(data, 1)
            if struct.unpack(">B", parsed_data["node_metadata_version"])[0] == 0:
                print("WARNING: node_metadata_version is 0, skipping node metadata")
            elif version < 28 and struct.unpack(">B", parsed_data["node_metadata_version"])[0] != 1:
                print("WARNING: node_metadata_version is not 1")
            elif version >= 28 and struct.unpack(">B", parsed_data["node_metadata_version"])[0] != 2:
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

        if version >= 25:
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

        pretty_data = {
            "was_compressed": None,
            "version": None, "flags": {"is_underground": None, "day_night_differs": None, "lighting_expired": None, "generated": None},
            "lighting_complete": {"nothing1": None, "nothing2": None, "nothing3": None, "nothing4": None,
                "night": {"X-": None, "Y-": None, "Z-": None, "Z+": None, "Y+": None, "X+": None},
                "day": {"X-": None, "Y-": None, "Z-": None, "Z+": None, "Y+": None, "X+": None}},
            "timestamp": None,
            "name_id_mapping_version": None, "name_id_mappings": [],
            "content_width": None, "params_width": None, "node_data": [],
            "node_metadata_version": None, "node_metadata": [],
            "static_object_version": None, "static_objects": [],
            "length_of_single_timer": None, "timers": []
        }

        pretty_data["was_compressed"] = parsed_data["was_compressed"]
        pretty_data["version"] = version

        pretty_data["flags"]["is_underground"] = bool(parsed_data["flags"] & 0x01)
        pretty_data["flags"]["day_night_differs"] = bool(parsed_data["flags"] & 0x02)
        pretty_data["flags"]["lighting_expired"] = bool(parsed_data["flags"] & 0x04)
        pretty_data["flags"]["generated"] = bool(parsed_data["flags"] & 0x08)

        # TODO: find a way to do this with less code: bool(parsed_data["lighting_complete"] & (1 << X)) is used every time

        pretty_data["lighting_complete"]["nothing1"] = bool(parsed_data["lighting_complete"] & (1 << 15))
        pretty_data["lighting_complete"]["nothing2"] = bool(parsed_data["lighting_complete"] & (1 << 14))
        pretty_data["lighting_complete"]["nothing3"] = bool(parsed_data["lighting_complete"] & (1 << 13))
        pretty_data["lighting_complete"]["nothing4"] = bool(parsed_data["lighting_complete"] & (1 << 12))
        pretty_data["lighting_complete"]["night"]["X-"] = bool(parsed_data["lighting_complete"] & (1 << 11))
        pretty_data["lighting_complete"]["night"]["Y-"] = bool(parsed_data["lighting_complete"] & (1 << 10))
        pretty_data["lighting_complete"]["night"]["Z-"] = bool(parsed_data["lighting_complete"] & (1 << 9))
        pretty_data["lighting_complete"]["night"]["Z+"] = bool(parsed_data["lighting_complete"] & (1 << 8))
        pretty_data["lighting_complete"]["night"]["Y+"] = bool(parsed_data["lighting_complete"] & (1 << 7))
        pretty_data["lighting_complete"]["night"]["X+"] = bool(parsed_data["lighting_complete"] & (1 << 6))
        pretty_data["lighting_complete"]["day"]["X-"] = bool(parsed_data["lighting_complete"] & (1 << 5))
        pretty_data["lighting_complete"]["day"]["Y-"] = bool(parsed_data["lighting_complete"] & (1 << 4))
        pretty_data["lighting_complete"]["day"]["Z-"] = bool(parsed_data["lighting_complete"] & (1 << 3))
        pretty_data["lighting_complete"]["day"]["Z+"] = bool(parsed_data["lighting_complete"] & (1 << 2))
        pretty_data["lighting_complete"]["day"]["Y+"] = bool(parsed_data["lighting_complete"] & (1 << 1))
        pretty_data["lighting_complete"]["day"]["X+"] = bool(parsed_data["lighting_complete"] & (1 << 0))

        pretty_data["timestamp"] = unpack("u32", parsed_data["timestamp"])

        pretty_data["name_id_mapping_version"] = unpack("u8", parsed_data["name_id_mapping_version"])

        for mapping in parsed_data["name_id_mappings"]:
            pretty_data["name_id_mappings"].append({"id": unpack("u16", mapping["id"]), "name": mapping["name"].decode("utf-8")})
        
        pretty_data["content_width"] = unpack("u8", parsed_data["content_width"])
        pretty_data["params_width"] = unpack("u8", parsed_data["params_width"])

        # TODO: support parsing node data fully dynamically depending on content_width and params_width

        param0_format = "u16" # 2 bytes since format version 24
        if pretty_data["content_width"] == 1:
            param0_format = "u8"

        for node in parsed_data["node_data"]:
            pretty_data["node_data"].append({"param0": unpack(param0_format, node["param0"]), "param1": unpack("u8", node["param1"]), "param2": unpack("u8", node["param2"])})

        if version < 23:
            pass # TODO: implement this!
        else:
            pretty_data["node_metadata_version"] = unpack("u8", parsed_data["node_metadata_version"])
            
            if pretty_data["node_metadata_version"] > 0:
                for metadata in parsed_data["node_metadata"]:
                    pretty_metadata = {"position": unpack("u16", metadata["position"]), "vars": []}

                    for var in metadata["vars"]:
                        is_private = False
                        if metadata["vars"]["is_private"]:
                            is_private = bool(metadata["vars"]["is_private"] & 0x01)
                        pretty_metadata["vars"].append({"key": var["key"].decode("utf-8"), "value": var["value"].decode("utf-8"), "is_private": is_private})
    
                    pretty_data["node_metadata"].append(pretty_metadata)

        pretty_data["static_object_version"] = unpack("u8", parsed_data["static_object_version"])

        for static_object in parsed_data["static_objects"]:
            pretty_data["static_objects"].append({"type": unpack("u8", static_object["type"]),
                "pos_x": unpack("s32", static_object["pos_x"])/10000,
                "pos_y": unpack("s32", static_object["pos_y"])/10000,
                "pos_z": unpack("s32", static_object["pos_z"])/10000,
                "data": parsed_data["data"] # TODO: create a separate StaticObject class to handle these and parse separately
            })

        pretty_data["length_of_single_timer"] = unpack("u8", parsed_data["length_of_single_timer"])

        for timer in parsed_data["timers"]:
            pretty_data["timers"].append({"position": unpack("u16", timer["position"]), "timeout": unpack("s32", timer["timeout"])/1000, "elapsed": unpack("s32", timer["elapsed"])/1000})

        return pretty_data

    def serialize(self, data=None, compressed=True):
        if data == None:
            data = self.data

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
                                if struct.unpack(">B", var["is_private"])[0] == 1:
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

        serialized_data = bytes(serialized_data)

        if compressed:
            serialized_data = serialized_data[:1] + zstd_compress(serialized_data[1:])

        return serialized_data

    def set_node(self, posxyz, param0, param1=0, param2=0):
        data = self.data

        pos_node = pos_get_node(posxyz)

        pos = (pos_node[2]*16*16 + pos_node[1]*16 + pos_node[0])

        if pos > 4095:
            return self

        mapping_id = None
        
        if isinstance(data["name_id_mappings"], list):
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

        self.data = data
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

    def get_mapblock(self, pos):
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT data FROM blocks WHERE x=? AND y=? AND z=?",
            (pos[0], pos[1], pos[2])
        )
        row = cursor.fetchone()
        if row:
            return MapBlock(pos, row[0])
        return None

    def set_mapblock(self, pos, blob):
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE blocks SET data=? WHERE x=? AND y=? AND z=?",
            (sqlite3.Binary(blob), pos[0], pos[1], pos[2])
        )
        self.conn.commit()

    def get_all_mapblocks(self):
        mapblocks = []
        for mapblock in self.list_mapblocks():
            mapblocks.append((mapblock[0], mapblock[1], mapblock[2], self.get_mapblock(mapblock)))

        return mapblocks
