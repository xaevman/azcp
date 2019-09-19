pipeline {
  agent any

  stages {
    stage('Build') { 
      steps {
        bat "go build -v -mod=vendor"
      }
    }
  }
}
