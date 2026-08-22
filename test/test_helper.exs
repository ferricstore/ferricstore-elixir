http_transport_options =
  case System.get_env("FERRICSTORE_CA_FILE") do
    path when is_binary(path) and path != "" -> [verify: :verify_peer, cacertfile: path]
    _unset -> [verify: :verify_none]
  end

Application.put_env(:ferricstore_sdk, :http_pool_transport_options, http_transport_options)
ExUnit.start(exclude: [:integration], assert_receive_timeout: 1_000)
