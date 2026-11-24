"""Métricas en Python puro: se calculan en el driver sobre pocas miles de filas."""


def _grupos(pares):
    i = 0
    while i < len(pares):
        j = i
        while j < len(pares) and pares[j][0] == pares[i][0]:
            j += 1
        yield i, j, sum(e for _, e in pares[i:j])
        i = j


def auc(etiquetas, puntajes):
    positivos = sum(etiquetas)
    negativos = len(etiquetas) - positivos
    if not positivos or not negativos:
        return None
    suma_rangos = sum((i + j + 1) / 2 * pos for i, j, pos in _grupos(sorted(zip(puntajes, etiquetas))))
    return (suma_rangos - positivos * (positivos + 1) / 2) / (positivos * negativos)


def precision_promedio(etiquetas, puntajes):
    positivos = sum(etiquetas)
    if not positivos:
        return None
    vp = 0
    resultado = 0.0
    for _, j, pos in _grupos(sorted(zip(puntajes, etiquetas), key=lambda x: -x[0])):
        vp += pos
        resultado += pos / positivos * vp / j
    return resultado


def matriz(etiquetas, alertas):
    vp = sum(1 for e, a in zip(etiquetas, alertas) if e and a)
    fp = sum(1 for e, a in zip(etiquetas, alertas) if not e and a)
    fn = sum(1 for e, a in zip(etiquetas, alertas) if e and not a)
    return vp, fp, fn, len(etiquetas) - vp - fp - fn


def umbral_por_costo(etiquetas, puntajes, costo_fn):
    """Umbral que minimiza costo_fn × brotes no detectados + falsas alarmas.
    Ante empate se prefiere el umbral más alto, que emite menos alertas."""
    def costo(umbral):
        _, fp, fn, _ = matriz(etiquetas, [p >= umbral for p in puntajes])
        return costo_fn * fn + fp
    return min((u / 100 for u in range(1, 100)), key=lambda u: (costo(u), -u))


def resumen(etiquetas, puntajes, umbral=None):
    fila = {"n": len(etiquetas), "positivos": sum(etiquetas), "auc": auc(etiquetas, puntajes),
            "precision_promedio": precision_promedio(etiquetas, puntajes), "umbral": umbral,
            "precision": None, "recall": None, "vp": None, "fp": None, "fn": None, "vn": None}
    if umbral is not None:
        vp, fp, fn, vn = matriz(etiquetas, [p >= umbral for p in puntajes])
        fila.update(vp=vp, fp=fp, fn=fn, vn=vn,
                    precision=vp / (vp + fp) if vp + fp else None,
                    recall=vp / (vp + fn) if vp + fn else None)
    return fila
