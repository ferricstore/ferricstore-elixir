defmodule FerricStore.Flow.Options.CollectionValidator do
  @moduledoc false

  alias FerricStore.DeadlineBudget

  alias FerricStore.Flow.Options.{
    ClaimCollectionValidator,
    NameCollectionValidator,
    QueryCollectionValidator
  }

  @spec validate(atom(), keyword()) :: :ok | {:error, term()}
  def validate(operation, opts), do: validate(operation, opts, nil)

  @spec validate(atom(), keyword(), DeadlineBudget.t() | nil) :: :ok | {:error, term()}
  def validate(operation, opts, budget) do
    with :ok <- ClaimCollectionValidator.validate(operation, opts, budget),
         :ok <- QueryCollectionValidator.validate(operation, opts, budget),
         do: NameCollectionValidator.validate(operation, opts, budget)
  end
end
