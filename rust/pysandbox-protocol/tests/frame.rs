use pysandbox_protocol::{
    DEFAULT_MAX_FRAME_BYTES, ExecuteRequest, ExecutionLimits, Frame, FrameKind, FuelOperation,
    ProtocolError, VfsRequest, WorkerRpcCall, decode_frame, decode_payload, encode_frame,
    encode_payload, read_frame, write_frame,
};

#[test]
fn frames_round_trip_with_an_opaque_byte_string_payload() {
    let frame = Frame::new(
        FrameKind::GuestCall,
        12,
        34,
        b"\xd9\x01\x02payload".to_vec(),
    );
    let encoded = encode_frame(&frame).unwrap();
    let decoded = decode_frame(&encoded, DEFAULT_MAX_FRAME_BYTES).unwrap();

    assert_eq!(decoded, frame);

    let value: cbor2::Value = cbor2::from_slice(&encoded).unwrap();
    let cbor2::Value::Map(fields) = value else {
        panic!("frame should encode as a CBOR map");
    };
    assert!(fields.iter().any(|(key, value)| {
        key == &cbor2::Value::from("payload")
            && value == &cbor2::Value::Bytes(b"\xd9\x01\x02payload".to_vec())
    }));
}

#[test]
fn vfs_requests_preserve_paths() {
    let request = VfsRequest::Read {
        path: "/packages/example.py".into(),
    };
    assert_eq!(
        decode_payload::<VfsRequest>(&encode_payload(&request).unwrap()).unwrap(),
        request
    );
}

#[test]
fn typed_payloads_round_trip_separately_from_the_frame() {
    let request = ExecuteRequest {
        program: "print('hello')".into(),
        limits: ExecutionLimits::default(),
    };

    assert_eq!(
        decode_payload::<ExecuteRequest>(&encode_payload(&request).unwrap()).unwrap(),
        request
    );
}

#[test]
fn worker_calls_preserve_their_fuel_operation() {
    let call = WorkerRpcCall {
        path: vec!["command".into(), "run".into()],
        fuel: Some(FuelOperation::Add {
            amount: 1_000,
            cap: Some(10_000),
        }),
        arguments: vec![1, 2, 3],
    };

    assert_eq!(
        decode_payload::<WorkerRpcCall>(&encode_payload(&call).unwrap()).unwrap(),
        call
    );
}

#[test]
fn oversized_frames_are_rejected_before_decoding() {
    let error = decode_frame(&[0; 1024], 1023).unwrap_err();

    assert!(matches!(
        error,
        ProtocolError::FrameTooLarge {
            actual: 1024,
            maximum: 1023
        }
    ));
}

#[test]
fn trailing_cbor_items_are_rejected() {
    let frame = Frame::new(FrameKind::HealthCheck, 0, 1, vec![]);
    let mut encoded = encode_frame(&frame).unwrap();
    encoded.push(0xf6);

    assert!(matches!(
        decode_frame(&encoded, DEFAULT_MAX_FRAME_BYTES),
        Err(ProtocolError::CborDecode(_))
    ));
}

#[tokio::test]
async fn frames_round_trip_through_an_async_cbor_sequence() {
    let first = Frame::new(FrameKind::HealthCheck, 0, 91, vec![]);
    let second = Frame::new(FrameKind::Shutdown, 0, 92, vec![]);
    let (mut client, mut server) = tokio::io::duplex(512);

    write_frame(&mut client, &first).await.unwrap();
    write_frame(&mut client, &second).await.unwrap();

    assert_eq!(
        read_frame(&mut server, DEFAULT_MAX_FRAME_BYTES)
            .await
            .unwrap(),
        first
    );
    assert_eq!(
        read_frame(&mut server, DEFAULT_MAX_FRAME_BYTES)
            .await
            .unwrap(),
        second
    );
}
