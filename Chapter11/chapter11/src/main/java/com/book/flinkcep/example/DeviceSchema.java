package com.book.flinkcep.example;

import java.io.IOException;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

public class DeviceSchema implements DeserializationSchema<DeviceEvent>, SerializationSchema<DeviceEvent>{

	private static final long serialVersionUID = 1051444497161899607L;

	@Override
	public TypeInformation<DeviceEvent> getProducedType() {
		return TypeExtractor.getForClass(DeviceEvent.class);
	}

	@Override
	public byte[] serialize(DeviceEvent element) {
		return element.toString().getBytes();
	}

	@Override
	public DeviceEvent deserialize(byte[] message) throws IOException {
		return DeviceEvent.fromString(new String(message));
	}

	@Override
	public boolean isEndOfStream(DeviceEvent nextElement) {
		return false;
	}

}
