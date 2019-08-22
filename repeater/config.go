package repeater

// Configuration for the repeater
//
// 'Exe' is the 'net-kafka-repeater' executable path
//
// Default 'WorkDir' (working directory) is current path
//
// 'Mode' specifies a 'sender' or a 'receiver'
//
// 'Relay' contains configurations for the netwokring
//
// 'Topic' containers kafka configurations
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
