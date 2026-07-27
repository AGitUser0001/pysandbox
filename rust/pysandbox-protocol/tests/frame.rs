use pysandbox_protocol::{Frame, FrameKind, decode_frame, encode_frame};

#[test]
fn frames_round_trip_through_cbor() {
    let frame = Frame::new(FrameKind::GuestCall, 12, 34, b"payload".to_vec());
    let decoded = decode_frame(&encode_frame(&frame).unwrap()).unwrap();

    assert_eq!(decoded, frame);
}
