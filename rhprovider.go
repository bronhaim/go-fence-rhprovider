package rhprovider

import (
	"bufio"
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sgotti/fence"
	"github.com/sgotti/fence/utils"
)

type RHAgentProvider struct {
	config *RHAgentProviderConfig
	agents map[string]*RHAgent
}

type RHAgentProviderConfig struct {
	Glob string
}

type RHAgent struct {
	Command string
	*fence.Agent
}

func newRHAgent() *RHAgent {
	return &RHAgent{Agent: fence.NewAgent()}
}

const (
	defaultGlob = "/usr/sbin/fence_*"
)

var defaultConfig = &RHAgentProviderConfig{Glob: defaultGlob}

func New(config *RHAgentProviderConfig) *RHAgentProvider {
	p := &RHAgentProvider{agents: make(map[string]*RHAgent)}
	if config != nil {
		p.config = config
	} else {
		p.config = defaultConfig
	}
	return p
}

type RHResourceAgent struct {
	Command    string
	XMLName    xml.Name      `xml:"resource-agent"`
	Name       string        `xml:"name,attr"`
	ShortDesc  string        `xml:"shortdesc,attr"`
	LongDesc   string        `xml:"longdesc"`
	VendorUrl  string        `xml:"vendor-url"`
	Parameters []RHParameter `xml:"parameters>parameter"`
	Actions    []RHAction    `xml:"actions>action"`
}

type RHParameter struct {
	Name      string    `xml:"name,attr"`
	Unique    bool      `xml:"unique,attr"`
	Required  bool      `xml:"required,attr"`
	ShortDesc string    `xml:"shortdesc"`
	Content   RHContent `xml:"content"`
}

type RHContent struct {
	ContentType string             `xml:"type,attr"`
	Default     string             `xml:"default,attr"`
	Options     []RHContentOptions `xml:"option"`
}

type RHContentOptions struct {
	Value string `xml:"value,attr"`
}

type RHAction struct {
	Name      string `xml:"name,attr"`
	OnTarget  string `xml:"on_target,attr"`
	Automatic string `xml:"automatic,attr"`
}

func parseMetadata(mdxml []byte) (*RHResourceAgent, error) {
	v := &RHResourceAgent{}
	err := xml.Unmarshal(mdxml, v)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (r *RHResourceAgent) toResourceAgent() (*RHAgent, error) {
	a := newRHAgent()

	a.Command = r.Command
	a.Name = r.Name
	a.ShortDesc = r.ShortDesc
	a.LongDesc = r.LongDesc

	for _, mdp := range r.Parameters {
		// If "action" parameter ignore it and set agent's DefaultAction
		if mdp.Name == "action" && mdp.Content.Default != "" {
			fa, err := StringToAction(mdp.Content.Default)
			if err != nil {
				// Ignore bad default action
			} else {
				a.DefaultAction = fa
			}
			continue
		}
		// If "port" parameter ignore it and set agent's DefaultAction
		if mdp.Name == "port" {
			a.MultiplePorts = true
			continue
		}
		// If "port" parameter ignore it and set agent's DefaultAction
		if mdp.Name == "separator" {
			continue
		}
		// TODO. All the metadatas reports unique = "0" but I think they should be unique...
		p := &fence.Parameter{Name: mdp.Name, Unique: mdp.Unique, Required: mdp.Required, Desc: mdp.ShortDesc}
		switch mdp.Content.ContentType {
		case "boolean":
			p.ContentType = fence.Boolean
			if mdp.Content.Default != "" {
				value, err := strconv.ParseBool(mdp.Content.Default)
				if err != nil {
					return nil, err
				}
				p.Default = value
			}
		case "string":
			p.ContentType = fence.String
			if mdp.Content.Default != "" {
				p.Default = mdp.Content.Default
			}
		case "select":
			p.HasOptions = true
			p.ContentType = fence.String
			if mdp.Content.Default != "" {
				p.Default = mdp.Content.Default
			}
		default:
			return nil, fmt.Errorf("Agent: %s, parameter: %s. Wrong content type: %s", a.Name, p.Name, mdp.Content.ContentType)
		}
		for _, o := range mdp.Content.Options {
			p.Options = append(p.Options, o.Value)
		}
		a.Parameters[p.Name] = p
	}
	for _, mda := range r.Actions {
		if mda.Name == "on" {
			if mda.Automatic == "1" {
				a.UnfenceAction = fence.On
			}
			if mda.OnTarget == "1" {
				a.UnfenceOnTarget = true
			}
		}
		fa, err := StringToAction(mda.Name)
		if err != nil {
			// Ignore unknown action
			continue
		}
		a.Actions = append(a.Actions, fa)
	}
	return a, nil
}

func (p *RHAgentProvider) LoadAgents(timeout time.Duration) error {
	p.agents = make(map[string]*RHAgent)

	files, err := filepath.Glob(p.config.Glob)
	if err != nil {
		return err
	}
	t := time.Now()
	nexttimeout := 0 * time.Second

	// TODO Detected duplicate agents? (agents returning the same name in metadata)
	for _, file := range files {
		if timeout != 0 {
			nexttimeout = timeout - time.Since(t)
			if nexttimeout < 0 {
				return errors.New("timeout")
			}
		}
		a, err := p.LoadAgent(file, nexttimeout)
		if err != nil {
			continue
		}
		p.agents[a.Name] = a
	}
	return nil
}

func (p *RHAgentProvider) LoadAgent(file string, timeout time.Duration) (*RHAgent, error) {
	cmd := exec.Command(file, "-o", "metadata")
	var b bytes.Buffer
	cmd.Stdout = &b
	err := cmd.Start()
	if err != nil {
		return nil, err
	}
	if timeout == time.Duration(0) {
		err = cmd.Wait()
	} else {
		err = utils.WaitTimeout(cmd, timeout)
	}
	if err != nil {
		return nil, err
	}

	mdxml := b.Bytes()
	mda, err := parseMetadata(mdxml)
	if err != nil {
		return nil, err
	}

	a, err := mda.toResourceAgent()
	if err != nil {
		return nil, fmt.Errorf("Agent \"%s\": %s", mda.Name, err)
	}

	a.Command = file

	return a, nil
}

func (p *RHAgentProvider) getRHAgent(name string) (*RHAgent, error) {
	a, ok := p.agents[name]
	if !ok {
		return nil, fmt.Errorf("Unknown agent: %s", name)
	}
	return a, nil
}

func (p *RHAgentProvider) GetAgents() (fence.Agents, error) {
	fagents := make(fence.Agents)
	for _, a := range p.agents {
		fagents[a.Name] = a.Agent
	}
	return fagents, nil
}

func (p *RHAgentProvider) GetAgent(name string) (*fence.Agent, error) {
	a, ok := p.agents[name]
	if !ok {
		return nil, fmt.Errorf("Unknown agent: %s", name)
	}
	return a.Agent, nil
}

func ActionToString(action fence.Action) string {
	switch action {
	case fence.On:
		return "on"
	case fence.Off:
		return "off"
	case fence.Reboot:
		return "reboot"
	case fence.Status:
		return "status"
	case fence.List:
		return "list"
	case fence.Monitor:
		return "monitor"
	}
	return ""
}
func StringToAction(action string) (fence.Action, error) {
	switch action {
	case "on", "enable":
		return fence.On, nil
	case "off", "disable":
		return fence.Off, nil
	case "reboot":
		return fence.Reboot, nil
	case "status":
		return fence.Status, nil
	case "list":
		return fence.List, nil
	case "monitor":
		return fence.Monitor, nil
	}
	return 0, fmt.Errorf("Unknown fence action: %s", action)
}

func (p *RHAgentProvider) run(ac *fence.AgentConfig, action string, timeout time.Duration) ([]byte, error) {
	a, err := p.getRHAgent(ac.Name)
	if err != nil {
		return nil, err
	}
	command := a.Command
	cmd := exec.Command(command)
	cmdstdin, err := cmd.StdinPipe()
	var b bytes.Buffer
	cmd.Stdout = &b
	if err != nil {
		return nil, err
	}
	err = cmd.Start()
	if err != nil {
		return nil, err
	}

	_, err = cmdstdin.Write([]byte(fmt.Sprintf("action=%s\n", action)))
	if err != nil {
		return nil, err
	}
	if ac.Port != "" {
		_, err = cmdstdin.Write([]byte(fmt.Sprintf("port=%s\n", ac.Port)))
		if err != nil {
			return nil, err

		}
	}
	for name, values := range ac.Parameters {
		for _, value := range values {
			_, err = cmdstdin.Write([]byte(fmt.Sprintf("%s=%s\n", name, value)))
			if err != nil {
				return nil, err
			}
		}
	}

	cmdstdin.Close()
	if timeout == time.Duration(0) {
		err = cmd.Wait()
	} else {
		err = utils.WaitTimeout(cmd, timeout)
	}

	if err != nil {
		return b.Bytes(), err
	}

	return b.Bytes(), nil
}

func (p *RHAgentProvider) Status(ac *fence.AgentConfig, timeout time.Duration) (fence.DeviceStatus, error) {
	_, err := p.run(ac, "status", timeout)
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			// Process exited with exit code != 0
			return fence.Ko, nil
		}
		return fence.Ko, err
	}

	return fence.Ok, nil
}

func (p *RHAgentProvider) Monitor(ac *fence.AgentConfig, timeout time.Duration) (fence.DeviceStatus, error) {
	_, err := p.run(ac, "monitor", timeout)
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			// Process exited with exit code != 0
			return fence.Ko, nil
		}
		return fence.Ko, err
	}

	return fence.Ok, nil
}

func (p *RHAgentProvider) List(ac *fence.AgentConfig, timeout time.Duration) (fence.PortList, error) {
	out, err := p.run(ac, "list", timeout)
	if err != nil {
		return nil, err
	}

	portList := make(fence.PortList, 0)
	reader := bufio.NewReader(bytes.NewReader(out))
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		var portName fence.PortName
		line := scanner.Text() // Println will add back the final '\n'
		split := strings.Split(line, ",")
		switch len(split) {
		case 1:
			portName = fence.PortName{Name: split[0]}
		case 2:
			portName = fence.PortName{Name: split[0], Alias: split[1]}
		default:
			return nil, fmt.Errorf("Wrong list format")
		}
		portList = append(portList, portName)
	}

	return portList, nil
}

func (p *RHAgentProvider) Run(ac *fence.AgentConfig, action fence.Action, timeout time.Duration) error {
	// Specify action only if action !- fence.None,
	// elsewhere the agent will run the default action
	var actionstr string
	if action != fence.None {
		actionstr = ActionToString(action)
	}

	_, err := p.run(ac, actionstr, timeout)
	return err
}
