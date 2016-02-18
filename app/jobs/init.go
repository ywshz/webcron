package jobs

import (
	"github.com/astaxie/beego"
	"doggie/app/models"
	"strings"
	"strconv"
	"fmt"
)

var DependencyMap = map[int]map[int]bool{}
var DependencyReverseMap = map[int]map[int]bool{}

func InitJobs() {
	list, _ := models.TaskGetList(1, 1000000, "status", 1)
	for _, task := range list {
		if task.TaskType == 0 {
			//	cron 任务
			job, err := NewJobFromTask(task)
			if err != nil {
				beego.Error("InitJobs:", err.Error())
				continue
			}
			AddJob(task.CronSpec, job)
		} else {
			//  依赖任务
			tasks := strings.Split(task.Dependency, ",")

			DependencyMap[task.Id] = make(map[int]bool)
			for _, idStr := range tasks {
				var id int
				id, _ = strconv.Atoi(idStr)

				DependencyMap[task.Id][id] = true

				_, exist := DependencyReverseMap[id]
				if !exist {
					DependencyReverseMap[id] = make(map[int]bool)
				}
				DependencyReverseMap[id][task.Id] = true;
			}
		}
	}

	fmt.Println("->Init DependencyMap : ", DependencyMap)
	fmt.Println("->Init DependencyReverseMap : ", DependencyReverseMap)
}
