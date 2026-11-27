param(
    [Parameter(Mandatory)][int]$Puerto,
    [Parameter(Mandatory)][string]$RutaDll
)

# Solo consulta la copia sintética ya actualizada; no modifica el modelo.
$ErrorActionPreference = 'Stop'
[System.Reflection.Assembly]::LoadFrom((Resolve-Path $RutaDll)) | Out-Null
$conexion = [Microsoft.AnalysisServices.AdomdClient.AdomdConnection]::new("Data Source=localhost:$Puerto")
$conexion.Open()
try {
    function Consultar([string]$Dax) {
        $comando = $conexion.CreateCommand()
        $comando.CommandText = $Dax
        $lector = $comando.ExecuteReader()
        try {
            $resultado = @{}
            if ($lector.Read()) {
                for ($i = 0; $i -lt $lector.FieldCount; $i++) {
                    $resultado[$lector.GetName($i).Trim('[', ']')] = $lector.GetValue($i)
                }
            }
            return $resultado
        } finally { $lector.Close(); $comando.Dispose() }
    }
    $marca = Consultar 'EVALUATE ROW("Modelo", [Versión del modelo])'
    if ($marca.Modelo -ne 'PRUEBA_NO_REAL') { throw 'Ejecutar exclusivamente sobre la copia sintética.' }
    $fila = 'ROW("N", [Semanas evaluadas], "TP", [Aciertos], "FP", [Falsas alarmas], "FN", [Positivas no detectadas], "TN", [Negativas correctas], "Recall", [Recall], "Precision", [Precisión], "SinDesenlace", [Sin desenlace (total)])'
    $sinBrote = Consultar "EVALUATE $fila"
    $todas = Consultar "EVALUATE CALCULATETABLE($fila, TREATAS({`"Todas las semanas`"}, Ambito[ambito]))"
    foreach ($caso in @(
        @{ Valores = $sinBrote; Esperado = @{ N=4; TP=1; FP=1; FN=1; TN=1; Recall=.5; Precision=.5; SinDesenlace=1 } },
        @{ Valores = $todas; Esperado = @{ N=5; TP=2; FP=1; FN=1; TN=1; Recall=(2/3); Precision=(2/3); SinDesenlace=1 } }
    )) {
        foreach ($campo in $caso.Esperado.Keys) {
            if ([math]::Abs([double]$caso.Valores[$campo] - $caso.Esperado[$campo]) -gt .000001) {
                throw "$campo incorrecto: $($caso.Valores[$campo]); esperado $($caso.Esperado[$campo])"
            }
        }
    }
    'DAX correcto en Power BI: exclusiones, conteos y razones en ambos ámbitos.'
    $sinBrote | ConvertTo-Json
    $todas | ConvertTo-Json
} finally { $conexion.Close(); $conexion.Dispose() }
