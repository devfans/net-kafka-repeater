package main

import (
  "os"
  "os/exec"
  "log"
  "flag"
  "time"

  "syscall"
  "strings"
  "strconv"

  "io/ioutil"
  "path/filepath"
  "encoding/json"

  "net-kafka-repeater/repeater"
)

type Manager struct {
  workDir       string

  config        *repeater.Config
  pid           int
  process       *os.Process

  configFile    string
  pidFile       string
  logFile       string
}

func (m *Manager) Setup(configFileName string) {
  m.configFile, _ = filepath.Abs(configFilename)
  log.Printf("Loading config file: %v", m.configFile)

  configFile, err := os.Open(m.configFile)
  if err != nil {
    log.Fatal("File error: ", err.Error())
  }
  defer configFile.Close()

  m.config = &repeater.Config {}
  jsonParser := json.NewDecoder(configFile)
  if err := jsonParser.Decode(m.config); err != nil {
    log.Fatal("Config error: ", err.Error())
  }

  m.workDir, err = filepath.Abs(m.config.WorkDir)
  if err != nil {
    log.Fatal("Invalid work directory: " + err.Error())
  }

  m.pidFile = path.join(m.workDir, m.config.PidFile)
  m.logFile = path.join(m.workDir, m.config.LogFile)
}

func (m *Manager) getProcess() bool {
  pidData, err := ioutil.ReadFile(m.pidFile)
  if err != nil {
    log.Println("Failed to read pid file: %v", m.pidFile)
    return false
  }

  m.pid, err = strconv.Atoi(string(pidData))
  if err != nil {
    log.Fatalln("Failed to read process pid")
  }

  m.process, err = os.FindProcess(m.pid)
  if err != nil {
    log.Fatalln("Failed to find process with pid: %v", m.pid)
  }

  err = m.process.Signal(syscall.Signal(0))
  if err != nil {
    return false
  }
  return true
}

func (m *Manager) spawn (exe string, pidFile string, logFile string, args ...string) {
  if _, err := os.Stats(logFile); err == nil {
    err = os.Rename(logFile, logFile + string(time.Now().Format(time.RFC3339)))
    if err != nil {
      log.Println("Failed to rename old log file: %v", logFile)
    }
  }

  logFileObject, err := os.OpenFile(logFile, os.O_RDWR|os.O_CREATE, 0755)
  if err != nil {
    log.Fatalln("Failed to create log file: %v", logFile)
  }
  defer logFileObject.Close()

  pidFileObject, err := os.OpenFile(pidFile, os.O_RDWR|os.O_CREATE, 0755)
  if err != nil {
    log.Fatalln("Failed to create pid file: %v", pidFile)
  }
  defer pidFileObject.Close()

  cmd = exec.Command(exe, args...)
  cmd.Stdout = logFileObject
  cmd.Stderr = logFileObject
  cmd.Start()

  _, err = pidFileObject.WriteString(strconv.Itoa(cmd.Process.Pid))
  if err != nil {
    log.Println("Failed to save pid file: %v", err)
  }
  pidFile.Sync()
}

func (m *Manager) Start() {
  if m.getProcess() {
    log.Fatalln("Process is already running")
  }
  exe, err := filepath.Abs(m.config.Exe)
  args := []string { m.configFile }
  m.spawn(exe, m.pidFile, m.logFile, args...)
  log.Println("Repeater process is started")
}

func (m *Manager Stop() {
  if m.getProcess() {
    _ = m.Process.Kill()
  }
  log.Println("Repeater is stopped")
}

func (m *Manager) Status() {
  if m.getProcess() {
    log.Println("Repeater is running.")
  } else {
    log.Println("Reater is stopped.")
  }
}

func main() {
  if (os.Args) < 2 {
    log.Fataln("Subcommand is required: start/stop/status")
  }
  subcommand := os.Args[1]
  flagSet := flag.NewFlagSet("subcommand", flag.ExitOnError)
  configFile := flagSet.String("config", "./config.json", "config json file")
  flagSet.Parse(os.Args[2:])

  manager := Manager {}
  manager.Setup(*configFile)

  switch strings.ToLower(subcommand) {
  case "start":
    manager.Start()
  case "stop":
    manager.Stop()
  case "status":
    manager.Status()
  default:
    log.Fatalln("Subcommand should be start/stop/status")
  }
}
