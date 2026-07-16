defmodule FerricStore.Architecture.TypesBoundaryTest do
  use FerricStore.Test.ArchitectureCase

  test "the public types facade isolates shallow and recursive normalization", %{calls: calls} do
    for {module, function} <- [
          {FerricStore.Types.MapKeyNormalizer, :normalize},
          {FerricStore.Types.ValueNormalizer, :normalize}
        ] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.Types and
                 call.callee_module == module and
                 call.callee_function == function
             end)
    end

    assert_no_calls(calls,
      from: [FerricStore.Types.MapKeyNormalizer],
      to: [FerricStore.Types.ValueNormalizer]
    )

    assert source_line_count("../../lib/ferric_store/types.ex") <= 100
    assert source_line_count("../../lib/ferric_store/types/map_key_normalizer.ex") <= 130
    assert source_line_count("../../lib/ferric_store/types/value_normalizer.ex") <= 220
  end
end
