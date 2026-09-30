def test_skeleton_compiles() -> None:
    import app.models  # noqa: F401
    import app.clock  # noqa: F401
    import app.config  # noqa: F401
    import app.auth  # noqa: F401
    import app.storage  # noqa: F401
    import app.aggregator  # noqa: F401
    import app.engine  # noqa: F401
    import app.api  # noqa: F401

    assert True
