defmodule FerricStore.FailureFormatter do
  @moduledoc false

  @spec exception_message(Exception.t(), binary()) :: binary()
  def exception_message(exception, fallback) when is_binary(fallback) do
    Exception.message(exception)
  rescue
    _error -> fallback
  catch
    _kind, _reason -> fallback
  end

  @spec inspect_term(term()) :: binary()
  def inspect_term(term) do
    inspect(term, limit: 20, printable_limit: 200)
  rescue
    _error -> "<unrenderable>"
  catch
    _kind, _reason -> "<unrenderable>"
  end
end
