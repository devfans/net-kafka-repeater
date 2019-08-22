package repeater

import (
  "io"
  "log"
  "net"
  "time"
  "bufio"
  "errors"

  "math/rand"
  "github.com/xtaci/kcp-go"
  "github.com/confluentinc/confluent-kafka-go/kafka"
)

// message struct
// FLAG  LEN  ID  DATA
//   2    4   4
const (
  PASSWORD = "NET-KAFKA-REPEATER"
  AUTH     = 0xef01
  DATA     = 0xfe01
  MAX_BUF_SIZE = 0xffffffff
  MAX_MSG_SIZE = 100000
)

// convert byte to int
func b2i(b []byte) uint32 {
  l := len(b)
  if l > 4 {
    log.Fatalf("Overflow %d bytes to uint32\n", l)
  }
  var total uint32 = 0
  for i := l - 1; i >= 0; i-- {
    total += uint32(b[i]) << uint(8*(l - i - 1))
  }
  return total
}

func i2b(n uint32) []byte {
  b := make([]byte, 4)
  for i := 3; i >= 0; i-- {
    b[i] = byte((n >> uint(8 * (3 - i))) & 0xff)
  }
  return b
}

type RelayConfig struct {
  Address       string
  Mode          string
  Password      string
  Ack           bool
}

type Receiver struct {
  producer      *TopicProducer
  config        *RelayConfig
}

type ISession interface {
  SendMessage([]byte) (int, error)
}

type Sender struct {
  consumer      *TopicConsumer
  config        *RelayConfig

  cs            ISession
}

type Session struct {
  conn          net.Conn

  Ip            string
  active        bool
  Login         bool
  sigs          chan bool
}

func (sess *Session) connectTcp(config *RelayConfig) {
  if sess.active {
    return
  }

  conn, err := net.Dial("tcp", config.Address)
  if err != nil {
    log.Println(err)
    sess.sigs <- false
    return
  }
  conn.(*net.TCPConn).SetKeepAlive(true)

  sess.conn = conn

}

func (sess *Session) SendMessage(data []byte) (int, error) {
  msg := sess.MakeMessage(data)
  return sess.Send(msg)
}

func (sess *Session) Send(data []byte) (int, error) {
  n, err := sess.conn.Write(data)
  if err != nil {
    sess.Close()
    if sess.sigs != nil {
      sess.sigs <- false
    }
    sess.active = false
  }
  return n, err
}

func (sess *Session) TcpConnect(config *RelayConfig, init chan bool) {
  go func(sigs chan bool) {
    starting := true
    for {
      if starting {
        starting = false
        sess.connectTcp(config)
        init <- true
      } else {
        <-sigs
        log.Printf("Connection broken detected, will reconnection in 2 seconds")
        time.Sleep(2 * time.Second)
        sess.connectTcp(config)
      }
    }
  } (sess.sigs)
}

func (sess *Session) KcpConnect(config *RelayConfig) {
  var err error
  sess.conn, err = kcp.Dial(config.Address)
  if err != nil {
    log.Fatalln("Failed to init kcp session: ", err)
  }
}

func (sess *Session) MakeAuthMessage (login bool, config *RelayConfig) []byte {
  msg := []byte{ byte((AUTH >> 8) & 0xff), byte(AUTH & 0xff)}
  if login {
    data := []byte(config.Password)
    msg = append(msg, i2b(uint32(10 + len(data)))...)
    msg = append(msg, i2b(rand.Uint32())...)
    msg = append(msg, data...)
  } else {
    msg = append(msg, i2b(10)...)
    msg = append(msg, i2b(rand.Uint32())...)
  }
  return msg
}

func (sess *Session) MakeAckMessage (id []byte) []byte {
  msg := []byte{ byte((DATA >> 8) & 0xff), byte(DATA & 0xff)}
  msg = append(msg, i2b(10)...)
  msg = append(msg, id...)
  return msg
}

func (sess *Session) MakeMessage (data []byte) []byte {
  msg := []byte{ byte((DATA >> 8) & 0xff), byte(DATA & 0xff)}
  msg = append(msg, i2b(uint32(10 + len(data)))...)
  msg = append(msg, i2b(rand.Uint32())...)
  msg = append(msg, data...)
  return msg
}

func (sess *Session) Handle (data []byte, size uint32, config *RelayConfig) (bool, []byte, error) {
  flag := uint32(data[0]) << 8
  flag += uint32(data[1])

  if flag == AUTH {
    password := string(data[10:])
    if password != config.Password {
      log.Printf("Invalid password: %v", password)
      sess.Login = false
      return false, sess.MakeAuthMessage(false, config), errors.New("Authentication Failure")
    } else {
      sess.Login = true
      log.Printf("Sender login success from %v", sess.Ip)
      return false, nil, nil
    }
  }
  if !sess.Login {
    return false, sess.MakeAuthMessage(false, config), errors.New("Login required")
  }
  if flag == DATA {
    log.Printf("New message: %v", string(data[6:]))
    return true, sess.MakeAckMessage(data[6:10]), nil
  } else {
    return false, nil, errors.New("Invalid data flag")
  }
}

func (sess *Session) Close() {
  if sess.conn == nil {
    return
  }
  switch c := sess.conn.(type) {
  case *net.TCPConn:
    c.Close()
  case *kcp.UDPSession:
    c.Close()
  }
}

func (r *Receiver) Start() {
  log.Printf("Mode: %v Target Address: %v", r.config.Mode, r.config.Address)

  if r.config.Mode == "kcp" {
    log.Println("Using kcp mode")
    go r.startKcp()
  } else {
    go r.startTcp()
  }
}

func (r *Receiver) startKcp() {
  server, err := kcp.Listen(r.config.Address)
  if err != nil {
    log.Fatalln("Failed to listen: ", err)
  }
  defer server.Close()
  for {
    conn, err := server.Accept()
    if err != nil {
      continue
    }
    incoming := conn.RemoteAddr().String()
    log.Printf("Incoming connection from %v", incoming)
    ip, _, _ := net.SplitHostPort(incoming)
    sess := &Session { Ip: ip, Login: false, conn: conn, active: true }
    go func (cs *Session) {
      csErr := r.handleSession(cs, r.producer)
      if csErr != nil {
        log.Println(err)
        cs.Close()
      }
    }(sess)
  }
}

func (r *Receiver) startTcp() {
  addr, err := net.ResolveTCPAddr("tcp", r.config.Address)
  if err != nil {
    log.Printf("Invalid listening address: %v", r.config.Address)
  }

  server, err := net.ListenTCP("tcp", addr)
  if err != nil {
    log.Fatalf("Failed to listen: %v", err)
  }
  defer server.Close()

  for {
    conn, err := server.AcceptTCP()
    if err != nil {
      continue
    }
    conn.SetKeepAlive(true)
    incoming := conn.RemoteAddr().String()
    log.Printf("Incoming connection from %v", incoming)
    ip, _, _ := net.SplitHostPort(incoming)
    sess := &Session { Ip: ip, Login: false, conn: conn, active: true }
    go func (cs *Session) {
      csErr := r.handleSession(cs, r.producer)
      if csErr != nil {
        log.Println(err)
        cs.Close()
      }
    }(sess)
  }
}

func (r *Receiver) handleSession(cs *Session, producer *TopicProducer) error {
  buf := bufio.NewReaderSize(cs.conn, MAX_BUF_SIZE)
  var msg [MAX_MSG_SIZE]byte
  for {
    meta, err := buf.Peek(6)
    log.Println(meta)
    if err != nil {
      log.Println(err)
      time.Sleep(time.Second)
      _, err = cs.Send([]byte("ping"))
      if err != nil {
        log.Println("Client connection closed")
        return err
      } else {
        continue
      }
    }
    size := b2i(meta[2:6])
    if uint32(buf.Buffered()) >= size {
      var data []byte
      if size > MAX_MSG_SIZE {
        data = make([]byte, size)
      } else {
        data = msg[0:size]
      }
      _, err := io.ReadFull(buf, data)
      if err != nil {
        return err
      }
      arrive, res, err := cs.Handle(data, size, r.config)
      if arrive {
        log.Printf("Received message: %v", string(data[10:]))
        producer.Process(data[10:])
      }
      if res != nil {
        _, handleErr := cs.Send(res)
        if err != nil {
          return handleErr
        }
      }
      if err != nil {
        return err
      }
    }
  }
}

func (s *Sender) Start() {
  log.Printf("Mode: %v Target Address: %v", s.config.Mode, s.config.Address)
  sigs := make(chan bool, 1)
  sess := &Session { Login: false, sigs: sigs }
  s.cs = sess


  if s.config.Mode == "kcp" {
    log.Println("Using kcp mode")
    sess.KcpConnect(s.config)
  } else {
    init := make(chan bool)
    sess.TcpConnect(s.config, init)
    <-init
  }

  sess.active = true
  incoming := sess.conn.RemoteAddr().String()
  log.Printf("Outgoing connection from %v", incoming)
  ip, _, _ := net.SplitHostPort(incoming)

  sess.Ip = ip
  sess.Login = false
  // Login
  auth := sess.MakeAuthMessage(true, s.config)
  sess.Send(auth)
  log.Println("Sent login request")

  // test
  go func() {
    for {
      time.Sleep(time.Second)
      if sess.active {
        data := sess.MakeMessage([]byte("Testing"))
        _, err := sess.Send(data)
        if err != nil {
          log.Println(err)
        }
      }
    }
  } ()

  for {
    msg := s.consumer.Poll(100)
    if msg != nil {
      s.Process(msg)
    }
  }
}

func (s *Sender) Process(msg *kafka.Message) {
  for {
    _, err := s.cs.SendMessage(msg.Value)
    if err != nil {
      log.Println("Failed to transfer message, will retry again")
      log.Println(err)
      time.Sleep(time.Second)
    } else {
      log.Printf("Transfered message: %v", string(msg.Value))
      return
    }
  }
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
    Ack:      true }
  var c map[string] interface {}
  if config.Relay != nil {
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
      log.Printf("Invalid mode: %v will use tcp instead.", modeName)
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

  ack, ok := c["ack"]
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


