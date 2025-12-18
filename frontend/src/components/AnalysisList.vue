<template>
  <div>
    <el-table :data="analysisResults" style="width: 100%">
      <el-table-column prop="content" label="内容" width="250"></el-table-column>
      <el-table-column prop="sentiment" label="情感" width="180"></el-table-column>
      <el-table-column prop="bot_score" label="水军得分"></el-table-column>
    </el-table>
  </div>
</template>

<script>
import axios from 'axios'

export default {
  data() {
    return {
      analysisResults: []
    }
  },
  mounted() {
    axios.get('http://127.0.0.1:8000/analysis')
      .then(response => {
        this.analysisResults = response.data
      })
      .catch(error => {
        console.error("There was an error fetching the analysis results:", error)
      })
  }
}
</script>
