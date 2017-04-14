error_chain! { 

    types {
        ClientError, ClientErrorKind, ClientResultExt, ClientResult;
    }  

    links {
        Token(::tokenerrors::TokenError, ::tokenerrors::TokenErrorKind);
    }
  }