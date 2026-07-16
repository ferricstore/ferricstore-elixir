defmodule FerricStore.Flow.ClaimNormalizerTest do
  use ExUnit.Case, async: true

  alias FerricStore.Flow.ClaimNormalizer

  test "normalizes valid compact claims and rejects unknown responses" do
    assert ClaimNormalizer.normalize(["id", "partition", "lease", 7]) ==
             {:ok,
              %{
                "id" => "id",
                "partition_key" => "partition",
                "lease_token" => "lease",
                "fencing_token" => 7
              }}

    assert ClaimNormalizer.normalize(["id", "partition", "lease", 7, "running"]) ==
             {:ok,
              %{
                "id" => "id",
                "partition_key" => "partition",
                "lease_token" => "lease",
                "fencing_token" => 7,
                "run_state" => "running"
              }}

    assert ClaimNormalizer.normalize(["id", "partition", "lease", 7, %{"a" => 1}]) ==
             {:ok,
              %{
                "id" => "id",
                "partition_key" => "partition",
                "lease_token" => "lease",
                "fencing_token" => 7,
                "attributes" => %{"a" => 1}
              }}

    assert ClaimNormalizer.normalize(["id", "lease", 7]) == {:error, :invalid_claim}

    assert ClaimNormalizer.normalize(:unknown) == {:error, :invalid_claim}
  end
end
