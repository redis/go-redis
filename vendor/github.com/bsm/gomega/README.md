# Gomega

This is a straight copy of the excellent [Gomega](http://onsi.github.io/gomega/) library,
stripped to the bare core to be free of third-party dependencies.

The following features are missing from this distribution:

- tests - Gomega itself is well tested but all test code was removed from this package
- MatchYAML matcher
- MatchXML matcher does not support utf-8 normalization
- ghttp/RespondWithProto matcher
- ghttp/VerifyProtoRepresenting matcher
