- Always prefer failure over silent degradation.
- Do not generate Python fallback implementations for Cython code.
- If Cython compilation would fail, return a compile-time error rather than adding runtime fallbacks.
- Never duplicate logic in Python and Cython unless explicitly requested.
- Performance > convenience. This is a performance-focused codebase, and we prioritize performance even if it means less convenient APIs, partially duplicated code or more complex code.
- No dynamic dispatch in hot paths - use static dispatch and explicit specialization.
- Do not gate imports behind try/except, failing at import time if a required dependency is missing rather than silently degrading functionality.
- The user is the architect of the system and should be involved in design decisions. This is a collaborative process, and the user should be empowered to make informed decisions about the codebase.

Everytime you break one of these rules, a kitten dies. Please follow these rules to keep the kittens alive, they're counting on you.