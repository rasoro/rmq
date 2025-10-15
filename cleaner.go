package rmq

import (
	"fmt"
	"log"
	"math"
)

type Cleaner struct {
	connection Connection
}

func NewCleaner(connection Connection) *Cleaner {
	return &Cleaner{connection: connection}
}

// Clean cleans the connection of the cleaner. This is useful to make sure no
// deliveries get lost. The main use case is if your consumers get restarted
// there will be unacked deliveries assigned to the connection. Once the
// heartbeat of that connection dies the cleaner can recognize that and remove
// those unacked deliveries back to the ready list. If there was no error it
// returns the number of deliveries which have been returned from unacked lists
// to ready lists across all cleaned connections and queues.
func (cleaner *Cleaner) Clean() (returned int64, err error) {
	connectionNames, err := cleaner.connection.getConnections()
	if err != nil {
		return 0, err
	}

	for _, connectionName := range connectionNames {
		hijackedConnection := cleaner.connection.hijackConnection(connectionName)
		switch err := hijackedConnection.checkHeartbeat(); err {
		case nil: // active connection
			continue
		case ErrorNotFound:
			n, err := cleanStaleConnection(hijackedConnection)
			if err != nil {
				return 0, err
			}
			returned += n
		default:
			return 0, err
		}
	}

	return returned, nil
}

func cleanStaleConnection(staleConnection Connection) (returned int64, err error) {
	queueNames, err := staleConnection.getConsumingQueues()
	if err != nil {
		return 0, err
	}

	for _, queueName := range queueNames {
		queue, err := staleConnection.OpenQueue(queueName)
		if err != nil {
			return 0, err
		}

		n, err := cleanQueue(queue)
		if err != nil {
			return 0, err
		}

		returned += n
	}

	if err := staleConnection.closeStaleConnection(); err != nil {
		return 0, err
	}

	// log.Printf("rmq cleaner cleaned connection %s", staleConnection)
	return returned, nil
}

func cleanQueue(queue Queue) (returned int64, err error) {
	returned, err = queue.ReturnUnacked(math.MaxInt64)
	if err != nil {
		return 0, err
	}
	if err := queue.closeInStaleConnection(); err != nil {
		return 0, err
	}
	// log.Printf("rmq cleaner cleaned queue %s %d", queue, returned)
	return returned, nil
}

// CleanInBatches is like Clean but it cleans the connection in batches. This is useful to avoid
// blocking the main thread for too long. And it clean connections keys to avoid too many keys
func (cleaner *Cleaner) CleanInBatches(pageCount int64, continueIfCleanError bool, logActive bool) (int64, error) {
	var cursor uint64
	var cleaned int64
	var err error
	for {
		var connectionNames []string
		connectionNames, cursor, err = cleaner.connection.getConnectionsPaginated(cursor, pageCount)
		if err != nil {
			return cleaned, fmt.Errorf(
				"error clean on getting connections paginated, cursor: %d, pageCount: %d, error: %w",
				cursor, pageCount, err)
		}
		batchCleaned := int64(0)
		for _, connectionName := range connectionNames {
			hijackedConnection := cleaner.connection.hijackConnection(connectionName)
			switch err := hijackedConnection.checkHeartbeat(); err {
			case nil: // active connection
				continue
			case ErrorNotFound:
				_, err := cleanConnection(hijackedConnection, pageCount)
				if err != nil {
					if continueIfCleanError {
						continue
					}
					return cleaned, fmt.Errorf("error on clean connection: %w", err)
				}
				cleaned += 1
				batchCleaned += 1
			default:
				if continueIfCleanError {
					continue
				}
				return cleaned, fmt.Errorf("error on check heartbeat: %w", err)
			}
		}
		if logActive {
			log.Printf("batch of connections cleaned %d", batchCleaned)
		}
		if cursor == 0 {
			break
		}
	}
	return cleaned, nil
}

func cleanConnection(connection Connection, pageCount int64) (int64, error) {
	var queueNames []string
	var cursor uint64
	var returned int64
	var err error
	for {
		queueNames, cursor, err = connection.getConsumingQueuesPaginated(cursor, pageCount)
		if err != nil {
			return 0, err
		}
		for _, queueName := range queueNames {
			queue, err := connection.OpenQueue(queueName)
			if err != nil {
				return 0, err
			}
			n, err := cleanQueue(queue)
			if err != nil {
				return 0, err
			}
			returned += n
		}

		if cursor == 0 {
			break
		}
	}

	if err := connection.closeStaleConnection(); err != nil {
		return 0, err
	}

	return returned, nil
}
