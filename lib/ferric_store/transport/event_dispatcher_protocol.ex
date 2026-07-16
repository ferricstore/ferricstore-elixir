defmodule FerricStore.Transport.EventDispatcherProtocol do
  @moduledoc false

  @type dispatch_result :: :ok | :dropped | :dropped_oldest
end
