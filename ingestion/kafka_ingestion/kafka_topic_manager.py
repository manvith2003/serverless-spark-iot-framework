"""
Kafka Topic Management for IoT Data Pipeline
Production-grade topic configuration with proper settings
"""

import json
import time
from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
import argparse


# Production Topic Configuration
TOPIC_CONFIGS = {
    # Raw data topics (one per data type)
    "iot-raw-data": {
        "partitions": 6,
        "replication_factor": 1,  # Set to 3 for production multi-broker
        "config": {
            "retention.ms": str(7 * 24 * 60 * 60 * 1000),  # 7 days
            "compression.type": "gzip",
            "cleanup.policy": "delete",
            "max.message.bytes": str(1 * 1024 * 1024),  # 1MB max message
        }
    },
    "iot-smartcity-raw": {
        "partitions": 3,
        "replication_factor": 1,
        "config": {
            "retention.ms": str(7 * 24 * 60 * 60 * 1000),
            "compression.type": "gzip",
            "cleanup.policy": "delete",
        }
    },
    "iot-healthcare-raw": {
        "partitions": 3,
        "replication_factor": 1,
        "config": {
            "retention.ms": str(14 * 24 * 60 * 60 * 1000),  # 14 days for healthcare
            "compression.type": "gzip",
            "cleanup.policy": "delete",
        }
    },
    "iot-industrial-raw": {
        "partitions": 3,
        "replication_factor": 1,
        "config": {
            "retention.ms": str(7 * 24 * 60 * 60 * 1000),
            "compression.type": "gzip",
            "cleanup.policy": "delete",
        }
    },
    # Validated data topic
    "iot-validated-data": {
        "partitions": 6,
        "replication_factor": 1,
        "config": {
            "retention.ms": str(14 * 24 * 60 * 60 * 1000),  # 14 days
            "compression.type": "snappy",  # Faster for processed data
            "cleanup.policy": "delete",
        }
    },
    # Critical alerts topic (high priority)
    "iot-alerts-critical": {
        "partitions": 3,
        "replication_factor": 1,
        "config": {
            "retention.ms": str(30 * 24 * 60 * 60 * 1000),  # 30 days
            "compression.type": "gzip",
            "cleanup.policy": "delete",
            "min.insync.replicas": "1",  # Set to 2 for production
        }
    },
    # Analytics results topic
    "iot-analytics-results": {
        "partitions": 3,
        "replication_factor": 1,
        "config": {
            "retention.ms": str(30 * 24 * 60 * 60 * 1000),  # 30 days
            "compression.type": "snappy",
            "cleanup.policy": "compact",  # Keep latest per key
        }
    },
    # Dead letter queue for failed messages
    "iot-dead-letter": {
        "partitions": 1,
        "replication_factor": 1,
        "config": {
            "retention.ms": str(90 * 24 * 60 * 60 * 1000),  # 90 days
            "compression.type": "gzip",
            "cleanup.policy": "delete",
        }
    },
}


class KafkaTopicManager:
    """Manage Kafka topics with production settings"""
    
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = None
    
    def connect(self):
        """Connect to Kafka cluster"""
        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id="iot-topic-manager"
            )
            print(f"✅ Connected to Kafka at {self.bootstrap_servers}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect: {e}")
            return False
    
    def list_topics(self):
        """List all existing topics"""
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                consumer_timeout_ms=5000
            )
            topics = consumer.topics()
            consumer.close()
            return topics
        except Exception as e:
            print(f"❌ Error listing topics: {e}")
            return set()
    
    def create_topic(self, name: str, config: dict) -> bool:
        """Create a single topic with specified configuration"""
        try:
            topic = NewTopic(
                name=name,
                num_partitions=config["partitions"],
                replication_factor=config["replication_factor"],
                topic_configs=config.get("config", {})
            )
            self.admin_client.create_topics([topic], validate_only=False)
            print(f"✅ Created topic: {name}")
            print(f"   Partitions: {config['partitions']}, Replication: {config['replication_factor']}")
            if config.get("config"):
                retention_days = int(config["config"].get("retention.ms", 0)) / (24*60*60*1000)
                print(f"   Retention: {retention_days:.0f} days, Compression: {config['config'].get('compression.type', 'none')}")
            return True
        except TopicAlreadyExistsError:
            print(f"⚠️  Topic already exists: {name}")
            return True
        except Exception as e:
            print(f"❌ Failed to create {name}: {e}")
            return False
    
    def create_all_topics(self) -> dict:
        """Create all IoT topics with production settings"""
        print("\n🚀 Creating IoT Kafka Topics with Production Settings\n")
        print("=" * 60)
        
        results = {}
        for topic_name, config in TOPIC_CONFIGS.items():
            results[topic_name] = self.create_topic(topic_name, config)
            time.sleep(0.5)  # Small delay between creations
        
        print("=" * 60)
        
        # Summary
        success = sum(1 for v in results.values() if v)
        print(f"\n📊 Summary: {success}/{len(results)} topics created/verified")
        
        return results
    
    def delete_topic(self, name: str) -> bool:
        """Delete a topic"""
        try:
            self.admin_client.delete_topics([name])
            print(f"🗑️  Deleted topic: {name}")
            return True
        except UnknownTopicOrPartitionError:
            print(f"⚠️  Topic not found: {name}")
            return False
        except Exception as e:
            print(f"❌ Failed to delete {name}: {e}")
            return False
    
    def describe_topics(self):
        """Describe all IoT topics"""
        print("\n📋 IoT Kafka Topics Configuration\n")
        print("=" * 80)
        
        existing_topics = self.list_topics()
        
        for topic_name in TOPIC_CONFIGS.keys():
            if topic_name in existing_topics:
                config = TOPIC_CONFIGS[topic_name]
                retention_days = int(config["config"].get("retention.ms", 0)) / (24*60*60*1000)
                print(f"✅ {topic_name}")
                print(f"   Partitions: {config['partitions']} | Replication: {config['replication_factor']}")
                print(f"   Retention: {retention_days:.0f} days | Compression: {config['config'].get('compression.type', 'none')}")
                print("-" * 80)
            else:
                print(f"❌ {topic_name} (not created)")
                print("-" * 80)
    
    def close(self):
        """Close admin client"""
        if self.admin_client:
            self.admin_client.close()


def main():
    parser = argparse.ArgumentParser(description="Kafka Topic Manager for IoT Pipeline")
    parser.add_argument("--broker", default="localhost:9092", help="Kafka broker address")
    parser.add_argument("--action", choices=["create", "list", "describe", "delete-all"],
                       default="create", help="Action to perform")
    
    args = parser.parse_args()
    
    manager = KafkaTopicManager(bootstrap_servers=args.broker)
    
    if not manager.connect():
        return
    
    try:
        if args.action == "create":
            manager.create_all_topics()
        elif args.action == "list":
            topics = manager.list_topics()
            print("\n📋 Existing Topics:")
            for topic in sorted(topics):
                if not topic.startswith("_"):  # Skip internal topics
                    print(f"   • {topic}")
        elif args.action == "describe":
            manager.describe_topics()
        elif args.action == "delete-all":
            print("⚠️  Deleting all IoT topics...")
            for topic_name in TOPIC_CONFIGS.keys():
                manager.delete_topic(topic_name)
    finally:
        manager.close()


if __name__ == "__main__":
    main()
