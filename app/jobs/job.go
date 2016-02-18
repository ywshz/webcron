package jobs

import (
	"bytes"
	"fmt"
	"github.com/astaxie/beego"
	"html/template"
	"os/exec"
	"runtime/debug"
	"strings"
	"sync"
	"time"
	"doggie/app/models"
	"doggie/app/mail"
	"github.com/satori/go.uuid"
	"io/ioutil"
	"os"
	"runtime"
)

var mailTpl *template.Template
var WORK_FOLDER string

func init() {
	mailTpl, _ = template.New("mail_tpl").Parse(`
	你好 {{.username}}，<br/>

<p>以下是任务执行结果：</p>

<p>
任务ID：{{.task_id}}<br/>
任务名称：{{.task_name}}<br/>       
执行时间：{{.start_time}}<br />
执行耗时：{{.process_time}}秒<br />
执行状态：{{.status}}
</p>
<p>-------------以下是任务执行输出-------------</p>
<p>{{.output}}</p>
<p>
--------------------------------------------<br />
本邮件由系统自动发出，请勿回复<br />
如果要取消邮件通知，请登录到系统进行设置<br />
</p>
`)

	WORK_FOLDER = beego.AppConfig.String("work.folder")
}

type Job struct {
	id         int
	name       string
	task       *models.Task
	runFunc    func() ([]byte, []byte, error)
	running    sync.Mutex
	status     int
	Concurrent bool
}

func NewJobFromTask(task *models.Task) (*Job, error) {
	if task.Id < 1 {
		return nil, fmt.Errorf("ToJob: 缺少id")
	}

	job := &Job{}

	switch task.CommandType {
	case "shell": job = NewCommandJob(task)
	case "python": job = NewPythonJob(task)
	case "go": job = NewGoJob(task)
	}

	job.task = task
	job.Concurrent = task.Concurrent == 0
	return job, nil
}

func NewCommandJob(task *models.Task) *Job {
	job := &Job{
		id:   task.Id,
		name: task.TaskName,
	}

	job.runFunc = func() ([]byte, []byte, error) {
		fileName := generateCommandFile(task.CommandType, task.Command)
		defer os.Remove(fileName)

		bufOut := new(bytes.Buffer)
		bufErr := new(bytes.Buffer)

		cmd := exec.Command(fileName)
		cmd.Stdout = bufOut
		cmd.Stderr = bufErr
		err := cmd.Run()

		return bufOut.Bytes(), bufErr.Bytes(), err
	}
	return job
}

func NewPythonJob(task *models.Task) *Job {
	job := &Job{
		id:   task.Id,
		name: task.TaskName,
	}
	job.runFunc = func() ([]byte, []byte, error) {
		fileName := generateCommandFile(task.CommandType, task.Command)
		defer os.Remove(fileName)

		bufOut := new(bytes.Buffer)
		bufErr := new(bytes.Buffer)
		cmd := exec.Command("python", "-f", fileName)
		cmd.Stdout = bufOut
		cmd.Stderr = bufErr
		err := cmd.Run()

		return bufOut.Bytes(), bufErr.Bytes(), err
	}
	return job
}

func NewGoJob(task *models.Task) *Job {
	job := &Job{
		id:   task.Id,
		name: task.TaskName,
	}
	job.runFunc = func() ([]byte, []byte, error) {
		fileName := generateCommandFile(task.CommandType, task.Command)
		defer os.Remove(fileName)

		bufOut := new(bytes.Buffer)
		bufErr := new(bytes.Buffer)
		cmd := exec.Command("go", "run", fileName)
		cmd.Stdout = bufOut
		cmd.Stderr = bufErr
		err := cmd.Run()
		return bufOut.Bytes(), bufErr.Bytes(), err
	}
	return job
}

func generateCommandFile(comdmandType,command string) string {
	fileName := WORK_FOLDER + string(os.PathSeparator) + uuid.NewV4().String()
	if comdmandType=="shell" && runtime.GOOS != "windows" {
		fileName += ".sh";
	} else {
		fileName += ".bat";
	}

	if comdmandType=="python" {
		fileName += ".py";
	}

	if comdmandType=="go" {
		fileName += ".go";
	}

	ioutil.WriteFile(fileName, bytes.NewBufferString(command).Bytes(), 0644)

	return fileName
}

func (j *Job) Status() int {
	return j.status
}

func (j *Job) GetName() string {
	return j.name
}

func (j *Job) GetId() int {
	return j.id
}

func (j *Job) Run() {
	defer func() {
		if err := recover(); err != nil {
			beego.Error(err, "\n", string(debug.Stack()))
		}
	}()

	t := time.Now()

	if j.Concurrent {
		j.running.Lock()
		defer j.running.Unlock()
	}

	if workPool != nil {
		workPool <- true
		defer func() {
			<-workPool
		}()
	}

	j.status = 1
	defer func() {
		j.status = 0
	}()

	bout, berr, err := j.runFunc()

	ut := time.Now().Sub(t) / time.Millisecond

	// 插入日志
	log := new(models.TaskLog)
	log.TaskId = j.id
	log.Output = string(bout)
	log.Error = string(berr)
	log.ProcessTime = int(ut)
	log.CreateTime = t.Unix()
	if err != nil {
		log.Status = -1
		log.Error = err.Error() + ":" + string(berr)
	}
	models.TaskLogAdd(log)

	// 更新上次执行时间
	j.task.PrevTime = t.Unix()
	j.task.ExecuteTimes++
	j.task.Update()

	// 任务完成后出发依赖关系
	if ( err == nil) {
		checkTasks,exist := DependencyReverseMap[j.task.Id]
		if exist {
			// find dependency
			for drkey, _ := range checkTasks {

				org,_ := models.TaskGetById(drkey)

				matchDependency := true
				for key,_ := range DependencyMap[drkey] {
					tk,_ := models.TaskGetById(key)
					if org.PrevTime > tk.PrevTime {
						matchDependency = false
					}
				}

				if matchDependency {
					//trigger the task
					beego.Debug("Trigger Task ", drkey)
					j,_  := NewJobFromTask(org)
					go j.Run()
				}
			}
		}
	}

	// 发送邮件通知
	if (j.task.Notify == 1 && err != nil) || j.task.Notify == 2 {
		user, uerr := models.UserGetById(j.task.UserId)
		if uerr != nil {
			return
		}

		title := ""
		if err != nil {
			title = fmt.Sprintf("任务执行结果通知 #%d: %s", j.task.Id, "失败")
		} else {
			title = fmt.Sprintf("任务执行结果通知 #%d: %s", j.task.Id, "成功")
		}
		data := make(map[string]interface{})
		data["task_id"] = j.task.Id
		data["username"] = user.UserName
		data["task_name"] = j.task.TaskName
		data["start_time"] = beego.Date(t, "Y-m-d H:i:s")
		data["process_time"] = float64(ut) / 1000
		if err != nil {
			data["status"] = "失败（" + err.Error() + "）"
			data["output"] = string(berr)
		} else {
			data["status"] = "成功"
			data["output"] = string(bout)
		}

		content := new(bytes.Buffer)
		mailTpl.Execute(content, data)
		ccList := make([]string, 0)
		if j.task.NotifyEmail != "" {
			ccList = strings.Split(j.task.NotifyEmail, "\n")
		}
		if !mail.SendMail(user.Email, user.UserName, title, content.String(), ccList) {
			beego.Error("发送邮件超时：", user.Email)
		}
	}
}
