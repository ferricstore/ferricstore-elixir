defmodule FerricStore.Result do
  @moduledoc false

  alias FerricStore.{Error, FailureFormatter}

  @spec unwrap(term()) :: term()
  def unwrap({:ok, value}), do: value
  def unwrap({:error, %Error{} = error}), do: {:error, error}
  def unwrap({:error, %{__exception__: true} = error}), do: {:error, error}
  def unwrap({:error, reason}), do: error(reason)
  def unwrap(value), do: value

  @spec error(term()) :: {:error, Error.t()}
  def error(%Error{} = error), do: {:error, error}
  def error(reason), do: {:error, to_error(reason)}

  defp to_error({status, value}) when status in [:error, :auth, :noperm, :busy, :bad_request] do
    %Error{message: FailureFormatter.inspect_term(value), status: status, raw: value}
  end

  defp to_error(reason), do: %Error{message: FailureFormatter.inspect_term(reason), raw: reason}
end
