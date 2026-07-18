defmodule FerricStore.SDK.Native.ServerContractTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol.{CapabilityContract, CommandSpec}
  alias FerricStore.SDK.Native.ServerContract
  alias FerricStore.Test.NativeServer

  test "accepts the exact current server contract" do
    assert :ok = ServerContract.validate(NativeServer.startup_payload())
  end

  test "requires the v0.8 response limit and compact codec advertisements" do
    startup = NativeServer.startup_payload()

    for path <- [
          ["capabilities", "limits", "max_response_bytes"],
          ["capabilities", "response_codecs", "compact_response_opcodes"]
        ] do
      incompatible = pop_in(startup, path) |> elem(1)

      assert {:error, {:incompatible_server_contract, _details}} =
               ServerContract.validate(incompatible)
    end
  end

  test "rejects malformed, unsupported, and multiply-owned compact response codecs" do
    invalid_tables = [
      %{"kv_get_v1" => [0x0101, 0x0101]},
      %{"future_codec_v1" => [0x0101]},
      %{"kv_get_v1" => [0x0101], "kv_mget_v1" => [0x0101]}
    ]

    Enum.each(invalid_tables, fn table ->
      startup =
        NativeServer.startup_payload(%{
          "capabilities" => %{
            "response_codecs" => %{"compact_response_opcodes" => table}
          }
        })

      assert {:error, {:incompatible_server_contract, %{invalid_compact_response_opcodes: _}}} =
               ServerContract.validate(startup)
    end)
  end

  test "rejects a session negotiated at a different protocol version" do
    startup = NativeServer.startup_payload(%{"version" => 2})

    assert {:error,
            {:incompatible_server_contract, %{protocol_version: 2, required_protocol_version: 1}}} =
             ServerContract.validate(startup)
  end

  test "rejects compression the SDK did not negotiate or implement" do
    startup = NativeServer.startup_payload(%{"compression" => "zlib"})

    assert {:error,
            {:incompatible_server_contract, %{compression: "zlib", required_compression: "none"}}} =
             ServerContract.validate(startup)
  end

  test "accepts the token-required compute completion contract" do
    startup =
      NativeServer.startup_payload(%{
        "capabilities" => %{
          "schemas" => %{
            "FETCH_OR_COMPUTE_RESULT" => %{
              "required" => ["key", "token", "value", "ttl_ms"]
            }
          }
        }
      })

    assert :ok = ServerContract.validate(startup)
  end

  test "the current flow mutation contract requires fencing inputs" do
    schemas = CapabilityContract.required_schemas()

    assert schemas["FLOW.SIGNAL"] == ["id", "signal"]

    assert schemas["FLOW.TRANSITION"] == [
             "id",
             "from_state",
             "to_state",
             "lease_token",
             "fencing_token"
           ]

    assert schemas["FLOW.COMPLETE"] == ["id", "lease_token", "fencing_token"]
  end

  test "rejects a server that omits any current SDK opcode" do
    opcodes =
      CommandSpec.all()
      |> Enum.reject(&(&1.name == "GET"))
      |> Enum.map(&%{"name" => &1.name, "opcode" => &1.opcode})

    startup =
      NativeServer.startup_payload(%{
        "capabilities" => %{"opcodes" => opcodes}
      })

    assert {:error, {:incompatible_server_contract, %{command: "GET", missing_opcode: 0x0101}}} =
             ServerContract.validate(startup)
  end

  test "rejects mandatory fields added to any current SDK schema" do
    startup =
      NativeServer.startup_payload(%{
        "capabilities" => %{
          "schemas" => %{
            "GET" => %{"required" => ["key", "tenant"]}
          }
        }
      })

    assert {:error,
            {:incompatible_server_contract,
             %{command: "GET", unsupported_required_fields: ["tenant"]}}} =
             ServerContract.validate(startup)
  end

  test "rejects a schema that omits an optional field used by the SDK" do
    startup =
      NativeServer.startup_payload(%{
        "capabilities" => %{
          "schemas" => %{
            "SET" => %{
              "required" => ["key", "value"],
              "fields" => [
                "key",
                "value",
                "ttl",
                "exat",
                "pxat",
                "nx",
                "xx",
                "keepttl",
                "deadline_ms"
              ]
            }
          }
        }
      })

    assert {:error,
            {:incompatible_server_contract, %{command: "SET", missing_supported_fields: ["get"]}}} =
             ServerContract.validate(startup)
  end

  test "accepts additive optional schema fields" do
    startup = NativeServer.startup_payload()
    fields = get_in(startup, ["capabilities", "schemas", "GET", "fields"])

    startup =
      put_in(startup, ["capabilities", "schemas", "GET", "fields"], fields ++ ["future_hint"])

    assert :ok = ServerContract.validate(startup)
  end

  test "rejects malformed supported schema field collections" do
    for fields <- [
          ["key", "deadline_ms", "key"],
          ["key", <<0xFF>>, "deadline_ms"],
          ["key", "deadline_ms" | :invalid_tail]
        ] do
      startup =
        NativeServer.startup_payload(%{
          "capabilities" => %{"schemas" => %{"GET" => %{"fields" => fields}}}
        })

      assert {:error,
              {:incompatible_server_contract,
               %{command: "GET", invalid_supported_fields: _reason}}} =
               ServerContract.validate(startup)
    end
  end

  test "rejects improper capability collections without raising" do
    invalid_startups = [
      NativeServer.startup_payload(%{
        "capabilities" => %{"protocol_versions" => [2 | :invalid_tail]}
      }),
      NativeServer.startup_payload(%{
        "capabilities" => %{
          "schemas" => %{"GET" => %{"required" => ["key" | :invalid_tail]}}
        }
      }),
      NativeServer.startup_payload(%{
        "capabilities" => %{
          "opcodes" => [%{"name" => "GET", "opcode" => 0x0101} | :invalid_tail]
        }
      })
    ]

    Enum.each(invalid_startups, fn startup ->
      assert {:error, {:incompatible_server_contract, details}} =
               ServerContract.validate(startup)

      assert is_map(details)
    end)
  end

  test "rejects malformed and duplicate advertised opcode entries" do
    valid_opcodes = get_in(NativeServer.startup_payload(), ["capabilities", "opcodes"])

    invalid_opcode_lists = [
      [:not_a_map | valid_opcodes],
      [%{"name" => "GET", "opcode" => 0x0101} | valid_opcodes]
    ]

    Enum.each(invalid_opcode_lists, fn opcodes ->
      startup = NativeServer.startup_payload(%{"capabilities" => %{"opcodes" => opcodes}})

      assert {:error, {:incompatible_server_contract, %{invalid_opcodes: _reason}}} =
               ServerContract.validate(startup)
    end)
  end

  test "bounds every advertised startup collection before validating its contents" do
    startup = NativeServer.startup_payload()

    for {path, limit} <- [
          {[], 32},
          {["capabilities"], 32},
          {["capabilities", "schemas"], 1_024}
        ] do
      oversized = update_path(startup, path, &pad_map(&1, limit, path))

      assert {:error,
              {:incompatible_server_contract,
               %{
                 invalid_collection: ^path,
                 reason: {:too_many_entries, %{limit: ^limit, observed: observed}}
               }}} = ServerContract.validate(oversized)

      assert observed == limit + 1
    end

    oversized_versions = List.duplicate(1, 17)

    assert {:error,
            {:incompatible_server_contract,
             %{
               invalid_capability: "protocol_versions",
               reason: {:too_many_entries, %{limit: 16, observed: 17}}
             }}} =
             NativeServer.startup_payload(%{
               "capabilities" => %{"protocol_versions" => oversized_versions}
             })
             |> ServerContract.validate()

    oversized_opcodes =
      Enum.map(1..1_025, fn opcode ->
        %{"name" => "EXTRA_#{opcode}", "opcode" => opcode}
      end)

    assert {:error,
            {:incompatible_server_contract,
             %{invalid_opcodes: {:too_many_entries, %{limit: 1_024, observed: 1_025}}}}} =
             NativeServer.startup_payload(%{
               "capabilities" => %{"opcodes" => oversized_opcodes}
             })
             |> ServerContract.validate()

    oversized_fields = Enum.map(1..129, &"field_#{&1}")

    assert {:error,
            {:incompatible_server_contract,
             %{
               command: "GET",
               invalid_required_fields: {:too_many_entries, %{limit: 128, observed: 129}}
             }}} =
             NativeServer.startup_payload(%{
               "capabilities" => %{
                 "schemas" => %{"GET" => %{"required" => oversized_fields}}
               }
             })
             |> ServerContract.validate()
  end

  test "rejects malformed entries even when all required capabilities are advertised" do
    valid_opcodes = get_in(NativeServer.startup_payload(), ["capabilities", "opcodes"])

    invalid_capabilities = [
      %{"protocol_versions" => [1, "1"]},
      %{"protocol_versions" => [1, 1]},
      %{
        "opcodes" => valid_opcodes ++ [%{"name" => "lowercase", "opcode" => 60_000}]
      },
      %{
        "opcodes" => valid_opcodes ++ [%{"name" => <<0xFF>>, "opcode" => 60_000}]
      },
      %{
        "schemas" => %{
          "GET" => %{"required" => ["key", <<0xFF>>]}
        }
      }
    ]

    Enum.each(invalid_capabilities, fn capabilities ->
      startup = NativeServer.startup_payload(%{"capabilities" => capabilities})

      assert {:error, {:incompatible_server_contract, details}} =
               ServerContract.validate(startup)

      assert is_map(details)
    end)
  end

  test "rejects duplicate numeric opcode ownership" do
    valid_opcodes = get_in(NativeServer.startup_payload(), ["capabilities", "opcodes"])

    startup =
      NativeServer.startup_payload(%{
        "capabilities" => %{
          "opcodes" => valid_opcodes ++ [%{"name" => "GET_ALIAS", "opcode" => 0x0101}]
        }
      })

    assert {:error,
            {:incompatible_server_contract,
             %{invalid_opcodes: {:duplicate_opcode, "GET_ALIAS", 0x0101}}}} =
             ServerContract.validate(startup)
  end

  test "rejects a malformed authentication requirement" do
    startup = NativeServer.startup_payload(%{"auth_required" => "true"})

    assert {:error,
            {:incompatible_server_contract,
             %{invalid_startup_field: "auth_required", value: "true"}}} =
             ServerContract.validate(startup)
  end

  defp pad_map(map, limit, path) do
    additions = limit - map_size(map) + 1

    Enum.reduce(1..additions, map, fn index, acc ->
      Map.put(acc, {:extra_contract_field, path, index}, index)
    end)
  end

  defp update_path(value, [], mapper), do: mapper.(value)
  defp update_path(value, path, mapper), do: update_in(value, path, mapper)
end
