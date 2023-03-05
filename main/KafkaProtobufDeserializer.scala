package scalapb.confluent

import com.google.protobuf.Message
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.protobuf.{
  KafkaProtobufDeserializer => CDeserializer
}
import scalapb.JavaProtoSupport

import scala.jdk.CollectionConverters.MapHasAsJava

trait KafkaProtobufDeserializer[A] {
  def deserialize(topic: String, bytes: Array[Byte]): A
}

object KafkaProtobufDeserializer {
  def apply[S, J <: Message](
      client: SchemaRegistryClient,
      props: Map[String, Any]
  )(implicit
      conv: JavaProtoSupport[S, J]
  ): KafkaProtobufDeserializer[S] =
    new KafkaProtobufDeserializer[S] {
      val de = new CDeserializer[J](client, props.asJava)

      def deserialize(topic: String, bytes: Array[Byte]): S =
        conv.fromJavaProto(de.deserialize(topic, bytes))
    }

}
