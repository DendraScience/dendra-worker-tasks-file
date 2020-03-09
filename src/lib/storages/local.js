const fs = require('fs').promises
const path = require('path')

class Local {
  constructor(options) {
    Object.assign(this, options)
  }

  async getFiles(options, tempPath, tempDir) {
    const { file_name: fileName } = options
    const storPath = path.resolve(this.path)
    const storFiles = await fs.readdir(storPath, { withFileTypes: true })
    const files = storFiles.filter(
      file =>
        file.name.startsWith(`${fileName}.`) &&
        file.name !== `${fileName}.manifest.yaml`
    )

    tempPath = path.resolve(tempPath, tempDir)

    await fs.mkdir(tempPath, { recursive: true })

    for (const file of files) {
      await fs.copyFile(
        path.join(storPath, file.name),
        path.join(tempPath, file.name)
      )
    }

    return files.map(file => ({
      name: file.name,
      path: path.join(tempPath, file.name)
    }))
  }
}

module.exports = Local
