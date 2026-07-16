defmodule FerricStore.Flow.EmptyBatchOptionsTest do
  use ExUnit.Case, async: true

  alias FerricStore.Flow

  test "empty batch no-ops validate options without requiring item defaults" do
    client = self()

    assert Flow.create_many(client, [], []) == []
    assert Flow.complete_many(client, [], []) == []
    assert Flow.value_mget(client, [], []) == []

    assert {:error,
            %FerricStore.Error{
              raw: {:unsupported_flow_options, :create_many, [:unknown]}
            }} = Flow.create_many(client, [], unknown: true)

    assert {:error,
            %FerricStore.Error{
              raw: {:invalid_request_option, :timeout, -1}
            }} = Flow.complete_many(client, [], timeout: -1)

    assert {:error,
            %FerricStore.Error{
              raw: {:invalid_flow_option, :value_mget, :codec, :expected_codec}
            }} = Flow.value_mget(client, [], codec: String)
  end

  test "empty create-many validates supplied defaults without invoking their codec" do
    assert {:error,
            %FerricStore.Error{
              raw: {:invalid_flow_option, :create_many, :attributes, :expected_map}
            }} = Flow.create_many(self(), [], attributes: :invalid)
  end
end
