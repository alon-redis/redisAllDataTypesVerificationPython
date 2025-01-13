import argparse
import redis
from redis.cluster import RedisCluster
import json
import datetime
import uuid
import sys
import time
from typing import Union, Dict, Any
from threading import Thread, Event

class RedisDataTester:
    def __init__(self, host: str = 'localhost', port: int = 6379, 
                 use_ssl: bool = False, protocol: int = 2,
                 cluster_mode: bool = False):
        """Initialize Redis connection with specified parameters."""
        try:
            connection_kwargs = {
                'host': host,
                'port': port,
                'ssl': use_ssl,
                'protocol': protocol,
                'decode_responses': True
            }
            
            if cluster_mode:
                self.redis_client = RedisCluster(startup_nodes=[{'host': host, 'port': port}],
                                               decode_responses=True,
                                               ssl=use_ssl,
                                               protocol=protocol)
            else:
                self.redis_client = redis.Redis(**connection_kwargs)
            
            # Test connection
            self.redis_client.ping()
            print(f"Successfully connected to Redis at {host}:{port}")
            
        except redis.ConnectionError as e:
            print(f"Failed to connect to Redis: {str(e)}")
            sys.exit(1)
        
        self.test_key_prefix = "test:"
        self.pubsub_thread = None
        self.pubsub_ready = Event()
        self.received_messages = []
    
    def flush_db(self):
        """Flush the current Redis database."""
        try:
            self.redis_client.flushdb()
            print("Successfully flushed Redis database")
        except redis.RedisError as e:
            print(f"Error flushing database: {str(e)}")
            sys.exit(1)

    def _generate_key(self, key_type: str) -> str:
        """Generate a unique key with prefix and type."""
        return f"{self.test_key_prefix}{key_type}:{uuid.uuid4()}"

    def _prepare_for_json(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert data structures to JSON-serializable format."""
        processed_data = {}
        for key, value in data.items():
            if 'value' not in value:
                processed_data[key] = value
                continue
            processed_value = value.copy()
            if isinstance(value['value'], set):
                processed_value['value'] = list(value['value'])
                processed_value['_type'] = 'set'
            elif isinstance(value['value'], bytes):                
                processed_value['value'] = list(value['value'])
                processed_value['_type'] = 'bytes'
            processed_data[key] = processed_value
        return processed_data

    def _restore_from_json(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Restore original data types from JSON format."""
        processed_data = {}
        for key, value in data.items():
            if 'value' not in value:
                processed_data[key] = value
                continue
            processed_value = value.copy()
            if value.get('_type') == 'set':
                processed_value['value'] = set(value['value'])
                del processed_value['_type']
            elif value.get('_type') == 'bytes':
                processed_value['value'] = bytes(value['value'])
                del processed_value['_type']
            processed_data[key] = processed_value
        return processed_data

    def _subscribe_handler(self, pubsub, channels):
        """Handle pub/sub message receiving in a separate thread."""
        for message in pubsub.listen():
            if message['type'] == 'message':
                self.received_messages.append([message['channel'], message['data']])
                if len(self.received_messages) == len(channels):
                    break
        self.pubsub_ready.set()

    def _replay_pubsub_messages(self, channels, messages):
        """Replay Pub/Sub messages during the verification phase."""
        time.sleep(0.1)  # Short delay to ensure subscription readiness
        for channel, message in zip(channels, messages):
            self.redis_client.publish(channel, message)

    def save_reference_data(self, reference_data: Dict[str, Any], filename: str = 'reference_data.json'):
        """Save reference data to a file for later verification."""
        try:
            serializable_data = self._prepare_for_json(reference_data)
            with open(filename, 'w') as f:
                json.dump(serializable_data, f, indent=2)
            print(f"Reference data saved to {filename}")
        except Exception as e:
            print(f"Error saving reference data: {str(e)}")
            raise

    def load_reference_data(self, filename: str = 'reference_data.json') -> Dict[str, Any]:
        """Load reference data from a file."""
        try:
            with open(filename, 'r') as f:
                data = json.load(f)
            return self._restore_from_json(data)
        except Exception as e:
            print(f"Error loading reference data: {str(e)}")
            sys.exit(1)
    
    def populate_data(self) -> Dict[str, Any]:
        """Populate Redis with different data types and return reference data."""
        reference_data = {}
        
        try:
            # Flush DB before population
            self.flush_db()
            
            # Basic Types
            # String
            string_key = self._generate_key("string")
            string_value = "Hello, Redis!"
            self.redis_client.set(string_key, string_value)
            reference_data['string'] = {'key': string_key, 'value': string_value}
            
            # Integer
            int_key = self._generate_key("int")
            int_value = 42
            self.redis_client.set(int_key, int_value)
            reference_data['int'] = {'key': int_key, 'value': str(int_value)}
            
            # List
            list_key = self._generate_key("list")
            list_value = ["item1", "item2", "item3"]
            self.redis_client.rpush(list_key, *list_value)
            reference_data['list'] = {'key': list_key, 'value': list_value}
            
            # Set
            set_key = self._generate_key("set")
            set_value = {"member1", "member2", "member3"}
            self.redis_client.sadd(set_key, *set_value)
            reference_data['set'] = {'key': set_key, 'value': set_value}

            # Hash
            hash_key = self._generate_key("hash")
            hash_value = {"field1": "value1", "field2": "value2"}
            self.redis_client.hset(hash_key, mapping=hash_value)
            reference_data['hash'] = {'key': hash_key, 'value': hash_value}

            # Sorted Set
            zset_key = self._generate_key("zset")
            zset_value = {"member1": 1.0, "member2": 2.0, "member3": 3.0}
            self.redis_client.zadd(zset_key, zset_value)
            reference_data['zset'] = {'key': zset_key, 'value': zset_value}
            
            # Geospatial
            geo_key = self._generate_key("geo")
            geo_value = {
                "New York": (40.7128, -74.0060),
                "London": (51.5074, -0.1278),
                "Tokyo": (35.6762, 139.6503)
            }
            for city, coords in geo_value.items():
                self.redis_client.geoadd(geo_key, [coords[1], coords[0], city])
            reference_data['geo'] = {'key': geo_key, 'value': geo_value}

            # Bitmap
            bitmap_key = self._generate_key("bitmap")
            self.redis_client.setbit(bitmap_key, 0, 1)
            self.redis_client.setbit(bitmap_key, 3, 1)
            self.redis_client.setbit(bitmap_key, 5, 1)
            bitmap_value = [1, 0, 0, 1, 0, 1]  # First 6 bits
            reference_data['bitmap'] = {'key': bitmap_key, 'value': bitmap_value}

            # HyperLogLog
            hll_key = self._generate_key("hll")
            hll_value = {"value1", "value2", "value3", "value4", "value5"}
            for val in hll_value:
                self.redis_client.pfadd(hll_key, val)
            hll_count = self.redis_client.pfcount(hll_key)
            reference_data['hyperloglog'] = {'key': hll_key, 'value': list(hll_value), 'count': hll_count}

            # Streams
            stream_key = self._generate_key("stream")
            stream_entries = [
                {"field1": "value1", "field2": "value2"},
                {"field1": "value3", "field2": "value4"}
            ]
            for entry in stream_entries:
                self.redis_client.xadd(stream_key, entry)
            reference_data['stream'] = {'key': stream_key, 'entries': stream_entries}

            # Pub/Sub
            pubsub = self.redis_client.pubsub()
            channels = ['channel1', 'channel2']
            messages = ['message1', 'message2']
            
            # Start subscriber thread
            pubsub.subscribe(*channels)
            self.pubsub_thread = Thread(target=self._subscribe_handler, args=(pubsub, channels))
            self.pubsub_thread.daemon = True
            self.pubsub_thread.start()
            
            # Wait a bit for subscription to be ready
            time.sleep(0.1)
            
            # Publish messages
            for channel, message in zip(channels, messages):
                self.redis_client.publish(channel, message)
            
            # Wait for messages to be received
            self.pubsub_ready.wait(timeout=5.0)
            pubsub.unsubscribe()
            
            reference_data['pubsub'] = {
                'channels': channels,
                'messages': messages,
                'received': self.received_messages
            }
            
            print("Successfully populated Redis with test data")
            self.save_reference_data(reference_data)
            return reference_data
            
        except redis.RedisError as e:
            print(f"Error populating Redis data: {str(e)}")
            sys.exit(1)

    def verify_data(self, reference_data: Dict[str, Any]) -> bool:
        """Verify that all populated data matches the reference data."""
        try:
            all_verified = True
            
            # Verify String
            string_data = self.redis_client.get(reference_data['string']['key'])
            if string_data != reference_data['string']['value']:
                print(f"String verification failed: {string_data} != {reference_data['string']['value']}")
                all_verified = False
            
            # Verify Integer
            int_data = self.redis_client.get(reference_data['int']['key'])
            if int_data != reference_data['int']['value']:
                print(f"Integer verification failed: {int_data} != {reference_data['int']['value']}")
                all_verified = False
            
            # Verify List
            list_data = self.redis_client.lrange(reference_data['list']['key'], 0, -1)
            if list_data != reference_data['list']['value']:
                print(f"List verification failed: {list_data} != {reference_data['list']['value']}")
                all_verified = False
            
            # Verify Set
            set_data = self.redis_client.smembers(reference_data['set']['key'])
            if set_data != reference_data['set']['value']:
                print(f"Set verification failed: {set_data} != {reference_data['set']['value']}")
                all_verified = False

            # Verify Hash
            hash_data = self.redis_client.hgetall(reference_data['hash']['key'])
            if hash_data != reference_data['hash']['value']:
                print(f"Hash verification failed: {hash_data} != {reference_data['hash']['value']}")
                all_verified = False

            # Verify Sorted Set
            zset_data = self.redis_client.zrange(reference_data['zset']['key'], 0, -1, withscores=True)
            zset_dict = {member: score for member, score in zset_data}
            if zset_dict != reference_data['zset']['value']:
                print(f"Sorted Set verification failed: {zset_dict} != {reference_data['zset']['value']}")
                all_verified = False
            
            # Verify Geospatial
            geo_key = reference_data['geo']['key']
            for city, coords in reference_data['geo']['value'].items():
                pos = self.redis_client.geopos(geo_key, city)[0]
                if abs(pos[0] - coords[1]) > 0.01 or abs(pos[1] - coords[0]) > 0.01:
                    print(f"Geospatial verification failed for {city}")
                    all_verified = False

            # Verify Bitmap
            bitmap_key = reference_data['bitmap']['key']
            for i, expected_bit in enumerate(reference_data['bitmap']['value']):
                actual_bit = self.redis_client.getbit(bitmap_key, i)
                if actual_bit != expected_bit:
                    print(f"Bitmap verification failed at position {i}")
                    all_verified = False

            # Verify HyperLogLog
            hll_key = reference_data['hyperloglog']['key']
            hll_count = self.redis_client.pfcount(hll_key)
            if hll_count != reference_data['hyperloglog']['count']:
                print(f"HyperLogLog verification failed: {hll_count} != {reference_data['hyperloglog']['count']}")
                all_verified = False

            # Verify Streams
            stream_key = reference_data['stream']['key']
            stream_entries = self.redis_client.xrange(stream_key)
            expected_entries = reference_data['stream']['entries']
            if len(stream_entries) != len(expected_entries):
                print(f"Stream verification failed: Expected {len(expected_entries)} entries, but got {len(stream_entries)}")
                print(f"Server response: {stream_entries}")
                print(f"Expected: {expected_entries}")
                all_verified = False
            
            # Verify Pub/Sub
            if 'pubsub' in reference_data:
                pubsub = self.redis_client.pubsub()
                channels = reference_data['pubsub']['channels']
                messages = reference_data['pubsub']['messages']

                # Replay messages
                replay_thread = Thread(target=self._replay_pubsub_messages, args=(channels, messages))
                replay_thread.start()

                # Subscribe and listen
                pubsub.subscribe(*channels)
                self.received_messages = []
                for message in pubsub.listen():
                    if message['type'] == 'message':
                        self.received_messages.append([message['channel'], message['data']])
                        if len(self.received_messages) == len(messages):
                            break
                pubsub.unsubscribe()

                if self.received_messages != reference_data['pubsub']['received']:
                    print(f"Pub/Sub verification failed: Server received = {self.received_messages}, Expected = {reference_data['pubsub']['received']}")
                    all_verified = False

            if all_verified:
                print("All data verified successfully")
            return all_verified
            
        except redis.RedisError as e:
            print(f"Error verifying Redis data: {str(e)}")
            return False

def main():
    parser = argparse.ArgumentParser(description='Redis Data Type Tester')
    parser.add_argument('--host', default='localhost', help='Redis host')
    parser.add_argument('--port', type=int, default=6379, help='Redis port')
    parser.add_argument('--ssl', action='store_true', help='Use SSL/TLS connection')
    parser.add_argument('--protocol', type=int, choices=[2, 3], default=2, 
                       help='RESP protocol version (2 or 3)')
    parser.add_argument('--cluster', action='store_true', help='Use cluster mode')
    parser.add_argument('--verify-only', action='store_true', 
                       help='Run only verification using existing reference data')
    args = parser.parse_args()

    # Initialize tester
    tester = RedisDataTester(
        host=args.host,
        port=args.port,
        use_ssl=args.ssl,
        protocol=args.protocol,
        cluster_mode=args.cluster
    )

    # Check Redis modules
    tester.check_redis_modules()

    # Run tests
    if args.verify_only:
        print("\\n=== Running Verification Only ===")
        reference_data = tester.load_reference_data()
        verification_success = tester.verify_data(reference_data)
    else:
        print("\\n=== Starting Redis Data Type Test ===")
        reference_data = tester.populate_data()
        
        print("\\n=== Verifying Data ===")
        verification_success = tester.verify_data(reference_data)
    
    if not verification_success:
        sys.exit(1)

if __name__ == '__main__':
    main()
