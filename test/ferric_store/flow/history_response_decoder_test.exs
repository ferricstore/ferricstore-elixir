defmodule FerricStore.Flow.HistoryResponseDecoderTest do
  use ExUnit.Case, async: true

  alias FerricStore.Codec.Term
  alias FerricStore.DeadlineBudget
  alias FerricStore.Flow.HistoryResponseDecoder

  test "raw history responses preserve event identity and normalize entries to tuples" do
    response = [["10-2", %{"event" => "created", "payload" => "raw"}]]

    assert [{"10-2", %{"event" => "created", "payload" => "raw"}}] =
             HistoryResponseDecoder.decode_raw(response, :history, DeadlineBudget.new(1_000))
  end

  test "application codecs decode record value fields inside history entries" do
    payload = Term.encode(%{tenant: "acme"})
    response = [["10-2", %{"event" => "created", "payload" => payload}]]

    assert [{"10-2", %{"event" => "created", "payload" => %{tenant: "acme"}}}] =
             HistoryResponseDecoder.decode(response, :history, Term)
  end

  test "history responses reject malformed entries and improper lists" do
    for {response, reason} <- [
          {[["10-2", %{}], ["missing-record"]], :invalid_history_entry},
          {[[10, %{}]], :invalid_history_entry},
          {[["10-2", %{}] | :invalid_tail], :expected_history_list},
          {%{}, :expected_history_list}
        ] do
      assert {:error,
              %FerricStore.Error{
                raw: {:invalid_flow_response, %{operation: :history, reason: ^reason}}
              }} =
               HistoryResponseDecoder.decode_raw(
                 response,
                 :history,
                 DeadlineBudget.new(1_000)
               )
    end
  end
end
