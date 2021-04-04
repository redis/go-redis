package proto_test

//
//func BenchmarkReader_ParseReplyResp3_Status(b *testing.B) {
//	benchmarkParseReplyResp3(b, "+OK\r\n", false)
//}
//
//func BenchmarkReader_ParseReplyResp3_Int(b *testing.B) {
//	benchmarkParseReplyResp3(b, ":1\r\n", false)
//}
//
//func BenchmarkReader_ParseReplyResp3_Error(b *testing.B) {
//	benchmarkParseReplyResp3(b, "-Error message\r\n", true)
//}
//
//func BenchmarkReader_ParseReplyResp3_String(b *testing.B) {
//	benchmarkParseReplyResp3(b, "$5\r\nhello\r\n", false)
//}
//
//func BenchmarkReader_ParseReplyResp3_Slice(b *testing.B) {
//	benchmarkParseReplyResp3(b, "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n", false)
//}
//
//func benchmarkParseReplyResp3(b *testing.B, reply string, wanterr bool) {
//	buf := new(bytes.Buffer)
//	for i := 0; i < b.N; i++ {
//		buf.WriteString(reply)
//	}
//	p := proto.NewRespReader(buf)
//	b.ResetTimer()
//
//	for i := 0; i < b.N; i++ {
//		_, err := p.ReadReply()
//		if !wanterr && err != nil {
//			b.Fatal(err)
//		}
//	}
//}
