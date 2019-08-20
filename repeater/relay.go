package repeater

const (
  PASSWORD = "NET-KAFKA-REPEATER"
  AUTH     = 0xef
  DATA     = 0xfe
)

type RelayConfig struct {
  Address       string
  Mode          string
  Password      string
  Ack           bool
}

type Receiver struct {
  producer      TopicProducer
  config        *RelayConfig
}

type Sender struct {
  consumer      TopicConsumer
  config        *RelayConfig
}

type TcpSession struct {
  Login         bool
  Ip            string
  conn          *net.TCPConn
}

func (cs *TcpSession) Close() {
  if cs.conn != nil {
    cs.conn.Close()
  }
}

func (r *Receiver) Start() {
  log.Println("Mode: %v Target Address: %v", r.Mode, r.Address)

  if r.Mode == "tcp" {
    go r.startTcp()
  }
}

func (r *Receiver) startTcp {
  addr, err := net.ResolveTCPAddr("tcp", r.Address)
  if err != nil {
    log.Println("Invalid listening address: %v", r.Address)
  }

  server, err := net.ListenTCP("tcp", addr)
  if err != nil {
    log.Fatalln("Failed to listen: %v", err)
  }
  defer server.Close()

  for {
    conn, err := server.AcceptTCP()
    if err != nil {
      continue
    }
    conn.SetKeepAlive(true)
    incoming = conn.RemoteAddr().String()
    log.Println("Incoming connection from %v", incoming)
    ip, _, _ := net.SplitHostPort(incoming)
    sess := &TcpSession { conn: conn, Ip: ip, Login: false }
    go func (cs *TcpSession) {
      csErr := receiver.handleTcpSession(cs)
      if csErr != nil {
        cs.Close()
      }
    }(sess)
  }
}

func (r *Receiver) handleTcpSession(cs *TcpSession) error {
}

func (s *Sender) Start() {
  log.Println("Mode: %v Target Address: %v", s.Mode, s.Address)
}

func NewReceiver(config *Config) *Receiver {
  receiver := & Receiver {}
  // parse config.relay
  receiver.producer = NewTopicProducer(config)
  receiver.config = parseRelay(config)
  return receiver
}

func parseRelay(config *Config) *RelayConfig {
  rc := & RelayConfig {
    Mode:     "tcp",
    Address:  "0.0.0.0:2000",
    Password: PASSWORD,
    Ack:      true
  }
  var c map[string] interface {}
  if config.Relay ! = nil {
    c = config.Relay
  } else {
    c = make(map[string]interface{})
  }

  mode, ok := c["mode"]
  if ok {
    modeName := mode.(string)
    if modeName == "tcp" || modeName == "kcp" {
      rc.Mode = mode.(string)
    } else {
      log.Println("Invalid mode: %v will use tcp instead.", modename)
    }
  }

  address, ok := c["address"]
  if ok {
    rc.Address = address.(string)
  }

  password, ok := c["password"]
  if ok {
    rc.Password = password.(string)
  }

  if ack, ok := c["ack"]
  if ok {
    rc.Ack = ack.(bool)
  }

  return rc
}

func NewSender(config *Config) *Sender {
  sender := & Sender {}

  sender.consumer = NewTopicConsumer(config)
  sender.config = parseRelay(config)
  return sender
}


