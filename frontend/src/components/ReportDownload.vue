<template>
  <div>
    <el-button type="primary" @click="downloadReport">下载日报</el-button>
    <el-button type="primary" @click="downloadWeeklyReport" style="margin-left: 10px;">下载周报</el-button>
  </div>
</template>

<script>
import axios from 'axios'

export default {
  methods: {
    downloadReport() {
      axios.get('http://127.0.0.1:8000/reports/daily', { responseType: 'blob' })
        .then(response => {
          const blob = new Blob([response.data], { type: 'application/pdf' })
          const link = document.createElement('a')
          link.href = URL.createObjectURL(blob)
          link.download = 'daily_report.pdf'
          link.click()
        })
        .catch(error => {
          console.error("There was an error downloading the report:", error)
        })
    },
    downloadWeeklyReport() {
      axios.get('http://127.0.0.1:8000/reports/weekly', { responseType: 'blob' })
        .then(response => {
          const blob = new Blob([response.data], { type: 'application/pdf' })
          const link = document.createElement('a')
          link.href = URL.createObjectURL(blob)
          link.download = 'weekly_report.pdf'
          link.click()
        })
        .catch(error => {
          console.error("There was an error downloading the report:", error)
        })
    }
  }
}
</script>
