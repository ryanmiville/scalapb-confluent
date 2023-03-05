package scalapb.confluent

import com.google.protobuf.Message
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.protobuf.{
  KafkaProtobufSerializer => CSerializer
}
import scalapb.JavaProtoSupport

import scala.jdk.CollectionConverters.MapHasAsJava

trait KafkaProtobufSerializer[A] {
  def serialize(topic: String, a: A): Array[Byte]
}

object KafkaProtobufSerializer {
  def apply[S, J <: Message](
      client: SchemaRegistryClient,
      props: Map[String, Any]
  )(implicit
      conv: JavaProtoSupport[S, J]
  ): KafkaProtobufSerializer[S] =
    new KafkaProtobufSerializer[S] {
      val ser = new CSerializer[J](client, props.asJava)

      def serialize(topic: String, a: S) =
        ser.serialize(topic, conv.toJavaProto(a))
    }

}
