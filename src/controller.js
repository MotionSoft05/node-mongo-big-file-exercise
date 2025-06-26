const fs = require("fs");
const csv = require("csv-parser");
const Records = require("./records.model");

const upload = async (req, res) => {
  const { file } = req;

  if (!file) {
    return res.status(400).json({
      success: false,
      error: "No file uploaded",
    });
  }

  const startTime = Date.now();
  let totalRecords = 0;
  let processedRecords = 0;
  let errors = 0;
  let batch = [];
  const BATCH_SIZE = 1000;

  try {
    console.log(`🚀 Iniciando procesamiento de: ${file.originalname}`);
    console.log(`📁 Archivo ubicado en: ${file.path}`);

    await new Promise((resolve, reject) => {
      const stream = fs.createReadStream(file.path).pipe(
        csv({
          skipEmptyLines: true,
          headers: [
            "id",
            "firstname",
            "lastname",
            "email",
            "email2",
            "profession",
          ],
        })
      );

      stream.on("data", async (row) => {
        try {
          totalRecords++;

          if (!row.id || !row.firstname || !row.email) {
            errors++;
            return;
          }

          const record = {
            id: parseInt(row.id),
            firstname: row.firstname.trim(),
            lastname: row.lastname ? row.lastname.trim() : "",
            email: row.email.trim().toLowerCase(),
            email2: row.email2 ? row.email2.trim().toLowerCase() : "",
            profession: row.profession ? row.profession.trim() : "",
          };

          batch.push(record);
          if (batch.length >= BATCH_SIZE) {
            stream.pause();

            try {
              await Records.insertMany(batch, { ordered: false });
              processedRecords += batch.length;

              console.log(`✅ Batch procesado: ${processedRecords} registros`);

              batch = [];

              stream.resume();
            } catch (batchError) {
              console.error("❌ Error en batch:", batchError.message);
              errors += batch.length;
              batch = [];
              stream.resume();
            }
          }
        } catch (rowError) {
          console.error("❌ Error procesando fila:", rowError.message);
          errors++;
        }
      });

      stream.on("error", (error) => {
        console.error("❌ Error en stream:", error.message);
        reject(error);
      });

      stream.on("end", async () => {
        try {
          if (batch.length > 0) {
            console.log(
              `🔄 Procesando último batch: ${batch.length} registros`
            );
            await Records.insertMany(batch, { ordered: false });
            processedRecords += batch.length;
          }

          console.log("✅ Stream completado");
          resolve();
        } catch (finalError) {
          console.error("❌ Error en batch final:", finalError.message);
          errors += batch.length;
          resolve();
        }
      });
    });

    const endTime = Date.now();
    const processingTime = (endTime - startTime) / 1000;
    const avgRecordsPerSecond = Math.round(processedRecords / processingTime);

    try {
      fs.unlinkSync(file.path);
      console.log("🗑️ Archivo temporal eliminado");
    } catch (unlinkError) {
      console.warn(
        "⚠️ No se pudo eliminar archivo temporal:",
        unlinkError.message
      );
    }

    const response = {
      success: true,
      totalRecords,
      processedRecords,
      errors,
      processingTime: `${processingTime.toFixed(1)}s`,
      avgRecordsPerSecond,
      filename: file.originalname,
      fileSize: `${(file.size / 1024 / 1024).toFixed(1)} MB`,
    };

    console.log("🎉 Procesamiento completado:", response);
    return res.status(200).json(response);
  } catch (error) {
    console.error("💥 Error general:", error);
    try {
      if (fs.existsSync(file.path)) {
        fs.unlinkSync(file.path);
      }
    } catch (unlinkError) {
      console.warn("⚠️ No se pudo eliminar archivo temporal tras error");
    }

    return res.status(500).json({
      success: false,
      error: error.message,
      totalRecords,
      processedRecords,
      errors,
    });
  }
};

const list = async (_, res) => {
  try {
    const data = await Records.find({}).sort({ _id: -1 }).limit(10).lean();

    const totalCount = await Records.countDocuments();

    return res.status(200).json({
      records: data,
      total: totalCount,
      showing: data.length,
    });
  } catch (err) {
    console.error("❌ Error obteniendo registros:", err);
    return res.status(500).json({
      success: false,
      error: err.message,
    });
  }
};

module.exports = {
  upload,
  list,
};
