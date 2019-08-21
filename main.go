package main

import (
  "os"
  "log"
  "path/filepath"
  "encoding/json"
  "net-kafka-repeater/repeater"
)

func main() {
  // read config
  configFileName := "config.json"
  if len(os.Args) > 1 {
    configFileName = os.Args[1]
  }

  configFileName, _ = filepath.Abs(configFileName)
  log.Println("Loading config file: %v", configFileName)

  configFile, err := os.Open(configFileName)
  if err != nil {
    log.Fatalln("Failed to read config file ", err.Error())
  }
  defer configFile.Close()

  parser := json.NewDecoder(configFile)
  var config repeater.Config
  if err := parser.Decode(&config); err != nil {
    log.Fatalln("Failed to parse config file: ", err.Error())
  }

  if config.Mode == "sender" {
    log.Println("Starting a new sender")
    sender := repeater.NewSender(&config)
    sender.Start()
  } else {
    log.Println("Starting a new receiver")
    receiver := repeater.NewReceiver(&config)
    receiver.Start()
  }

  <-make(chan byte)
}
