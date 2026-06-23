exclude = if System.get_env("FERRICSTORE_INTEGRATION") == "1", do: [], else: [:integration]
ExUnit.start(exclude: exclude)
