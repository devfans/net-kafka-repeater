package repeater

type Config struct {
  Exe            string
  PidFile        string
  LogFile        string
  WorkDir        string

  Mode           string  `sender or receiver`

  // Relay
  Relay          map[string] interface{}

  // Kafka Topic
  Topic          map[string] interface{}
}
