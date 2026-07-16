defmodule FerricStore.SDK.Native.ClientCredentialOptions do
  @moduledoc false

  @max_username_bytes 1_024
  @max_password_bytes 4_096

  @spec validate(keyword()) :: :ok | {:error, {atom(), term()}}
  def validate(opts) when is_list(opts) do
    with :ok <- validate_username(Keyword.get(opts, :username)) do
      validate_password(Keyword.get(opts, :password))
    end
  end

  defp validate_username(nil), do: :ok
  defp validate_username("" = username), do: {:error, {:username, username}}

  defp validate_username(username) when is_binary(username) do
    cond do
      byte_size(username) > @max_username_bytes ->
        too_large(:username, username, @max_username_bytes)

      not String.valid?(username) ->
        {:error, {:username, %{reason: :invalid_utf8}}}

      true ->
        :ok
    end
  end

  defp validate_username(username), do: {:error, {:username, username}}

  defp validate_password(nil), do: :ok

  defp validate_password(password) when is_binary(password) do
    if byte_size(password) <= @max_password_bytes,
      do: :ok,
      else: too_large(:password, password, @max_password_bytes)
  end

  defp validate_password(password), do: {:error, {:password, password}}

  defp too_large(key, value, limit) do
    {:error, {key, %{reason: :too_large, bytes: byte_size(value), limit: limit}}}
  end
end
