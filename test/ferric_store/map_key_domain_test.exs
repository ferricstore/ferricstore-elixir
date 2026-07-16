defmodule FerricStore.MapKeyDomainTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol
  alias FerricStore.Test.ObservableString
  alias FerricStore.Types

  test "normalization accepts only binary and atom map keys without application callbacks" do
    custom_key = %ObservableString{owner: self()}

    assert {:error, {:invalid_map_key, ^custom_key}} =
             Types.normalize_map_keys_result(%{custom_key => "value"})

    assert {:error, {:invalid_map_key, 1}} = Types.normalize_map_keys_result(%{1 => "value"})
    refute_received :string_chars_called
  end

  test "wire encoding accepts only binary and atom map keys without application callbacks" do
    custom_key = %ObservableString{owner: self()}

    assert_raise ArgumentError, ~r/cannot encode native map key/, fn ->
      Protocol.encode_value(%{custom_key => "value"})
    end

    assert_raise ArgumentError, ~r/cannot encode native map key/, fn ->
      Protocol.encode_value(%{1 => "value"})
    end

    refute_received :string_chars_called
  end
end
