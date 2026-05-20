#!/usr/bin/env python3
"""
Script para transformar archivos Markdown a JSON estructurado.
Convierte tablas de biomarcadores en formato JSON estructurado.

Enfoque:
- Metadata desde Markdown: NDA, Drug, Area, Biomarker, Sections
- Content desde Markdown: Labeling Text extraído de la columna 5 del archivo Markdown
- Clave única compuesta: NDA + Drug Name + Biomarker para diferenciar entradas duplicadas
- Manejo robusto de saltos de página: contenido que pertenece al registro anterior se acumula correctamente
"""

import re
import json
import os
from typing import List, Dict, Optional, Set, Tuple

# Marker ya no se usa - todo se extrae del archivo Markdown


def create_composite_key(nda: str, drug_name: str, biomarker: str) -> str:
    """
    Crea una clave única compuesta: NDA + Drug Name + Biomarker.
    Esto permite diferenciar entradas como Abemaciclib que aparecen varias veces
    con el mismo NDA pero distintos biomarcadores.
    """
    parts = [nda or "", drug_name or "", biomarker or ""]
    return "|||".join(parts)  # Separador único que no aparece en los datos


def clean_html_content(text: str) -> str:
    """
    Limpia el contenido HTML preservando la estructura de listas y párrafos del PDF,
    pero eliminando etiquetas de formato redundantes.
    
    NOTA: Esta función se mantiene para el 0.2% de casos que aún tienen HTML.
    Para la mayoría de casos (texto plano), usar clean_text_content().
    """
    if not text:
        return ""
    
    # Convertir <br/> y <br> a saltos de línea simples
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    
    # Convertir <p> y </p> a saltos de línea dobles (párrafos)
    text = re.sub(r'<p[^>]*>', '\n\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '', text, flags=re.IGNORECASE)
    
    # Preservar estructura de listas: convertir <ul>/<ol> a saltos de línea
    text = re.sub(r'<ul[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<ol[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</(ul|ol)>', '\n', text, flags=re.IGNORECASE)
    
    # Convertir <li> a bullet points con indentación
    text = re.sub(r'<li[^>]*>', '  * ', text, flags=re.IGNORECASE)
    text = re.sub(r'</li>', '\n', text, flags=re.IGNORECASE)
    
    # Eliminar etiquetas de formato redundantes (preservar contenido)
    text = re.sub(r'</?(strong|b)[^>]*>', '', text, flags=re.IGNORECASE)
    text = re.sub(r'</?(i|em)[^>]*>', '', text, flags=re.IGNORECASE)
    
    # Convertir encabezados a texto con saltos de línea
    text = re.sub(r'<h[1-6][^>]*>', '\n\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</h[1-6]>', '\n\n', text, flags=re.IGNORECASE)
    
    # Eliminar cualquier otra etiqueta HTML
    text = re.sub(r'<[^>]+>', '', text)
    
    # Decodificar entidades HTML comunes
    text = text.replace('&gt;', '>')
    text = text.replace('&lt;', '<')
    text = text.replace('&amp;', '&')
    text = text.replace('&nbsp;', ' ')
    text = text.replace('&reg;', '®')
    text = text.replace('&trade;', '™')
    
    # Limpiar espacios múltiples pero preservar saltos de línea
    lines = text.split('\n')
    cleaned_lines = []
    for line in lines:
        # Limpiar espacios/tabs múltiples pero preservar indentación de listas
        cleaned_line = re.sub(r'[ \t]+', ' ', line).strip()
        if cleaned_line:
            cleaned_lines.append(cleaned_line)
    
    # Unir líneas y limpiar saltos de línea múltiples (máximo 2 consecutivos)
    text = '\n'.join(cleaned_lines)
    text = re.sub(r'\n{3,}', '\n\n', text)
    
    return text.strip()


def clean_text_content(text: str) -> str:
    """
    Limpia texto plano: normalización de espacios y saltos de línea.
    
    Detecta HTML automáticamente y usa clean_html_content() si es necesario (para el 0.2% de casos).
    Para la mayoría de casos (99.8%), hace limpieza simple de texto plano.
    """
    if not text:
        return ""
    
    # Detectar HTML (para el 0.2% de casos que aún tienen HTML)
    has_html = bool(re.search(r'<(strong|p|br|ul|li|h[1-6]|b|i|em)[\s>]', text, re.IGNORECASE))
    
    if has_html:
        # Fallback a limpieza de HTML
        return clean_html_content(text)
    
    # Limpieza simple de texto plano
    # Normalizar saltos de línea múltiples (máximo 2 consecutivos)
    text = re.sub(r'\n{3,}', '\n\n', text)
    
    # Limpiar espacios múltiples pero preservar saltos de línea
    lines = text.split('\n')
    cleaned_lines = []
    for line in lines:
        # Limpiar espacios/tabs múltiples
        cleaned_line = re.sub(r'[ \t]+', ' ', line).strip()
        if cleaned_line:
            cleaned_lines.append(cleaned_line)
    
    return '\n'.join(cleaned_lines).strip()


def clean_text(text: str) -> str:
    """
    Limpia el texto básico: espacios múltiples.
    """
    if not text:
        return ""
    
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def is_nda_number(cell_text: str) -> bool:
    """
    Valida si el texto es un número NDA/ANDA/BLA válido.
    
    Acepta múltiples formatos:
    - "213871, 01/14/2022" (número primero, luego fecha)
    - "12/16/2020, 020298" (fecha primero, luego número)
    - "213871 01/14/2022" (sin coma)
    """
    if not cell_text or not cell_text.strip():
        return False
    
    cell_text = cell_text.strip()
    
    # Patrones: número primero, luego fecha
    patterns = [
        r'^\d{4,7}[,\s\.]\s*\d{1,2}/\d{1,2}/\d{4}$',
        r'^\d{4,7}\s*[,\s\.]\s*\d{1,2}/\d{1,2}/\d{4}',
    ]
    
    for pattern in patterns:
        if re.match(pattern, cell_text):
            return True
    
    # Patrón adicional: fecha primero, luego número (ej: "12/16/2020, 020298")
    date_first_pattern = r'^\d{1,2}/\d{1,2}/\d{4}[,\s\.]\s*\d{4,7}$'
    if re.match(date_first_pattern, cell_text):
        return True
    
    return False


def is_table_header_text(text: str) -> bool:
    """
    Filtro para ignorar filas que repiten los encabezados de la tabla.
    Detecta artefactos de saltos de página del PDF.
    """
    if not text:
        return False
    
    text_lower = text.lower().strip()
    
    # Patrones de encabezados de tabla
    header_patterns = [
        'nda/anda/bla number',
        'label version date',
        'drug',
        'therapeutic area',
        'biomarker',
        'labeling sections',
        'labeling text',
    ]
    
    # Verificar si el texto coincide con algún encabezado
    for pattern in header_patterns:
        if pattern in text_lower and len(text_lower) < 200:  # Encabezados son cortos
            return True
    
    return False


def is_section_text(text: str) -> bool:
    """
    Heurística genérica para detectar si un texto contiene secciones de labeling.
    
    Las secciones típicamente:
    - Contienen palabras clave como 'Reactions', 'Studies', 'Pharmacology', etc.
    - Tienen comas (múltiples secciones separadas por comas)
    - Son relativamente cortas (menos de 200 caracteres) - NO es el contenido completo
    - NO contienen HTML tags extensos (<p>, <b>, etc.) - eso es contenido, no secciones
    - NO son números de NDA, nombres de fármacos cortos, o biomarkers
    """
    if not text:
        return False
    
    # Secciones reconocibles cortas (una sola palabra) - verificar primero
    short_sections = ['Warnings', 'Precautions', 'Usage', 'Dosage', 'Indications']
    text_stripped = text.strip()
    if text_stripped in short_sections:
        return True
    
    # Para textos más largos, aplicar filtros más estrictos
    if len(text) < 10:
        return False
    
    # EXCLUIR: Si tiene mucho HTML, es contenido, no secciones
    html_tags = ['<p>', '<b>', '<strong>', '<ul>', '<li>', '<br>', '<a href']
    if sum(1 for tag in html_tags if tag in text) > 2:
        return False
    
    # EXCLUIR: Si es muy largo (>200 chars), probablemente es contenido completo, no solo secciones
    if len(text) > 200:
        return False
    
    # Palabras clave de secciones comunes (deben aparecer como palabras completas o con contexto)
    section_keywords = [
        'Reactions', 'Studies', 'Pharmacology', 'Usage', 'Administration',
        'Warnings', 'Precautions', 'Dosage', 'Populations', 'Indications',
        'Contraindications', 'Boxed Warning', 'Clinical'
    ]
    
    # Debe contener al menos una palabra clave de sección
    has_keyword = any(keyword in text for keyword in section_keywords)
    
    if not has_keyword:
        return False
    
    # Debe tener características de texto de secciones:
    # - Tiene comas (múltiples secciones separadas) Y contiene palabras clave
    # - O es una sola sección reconocible (ej: "Warnings", "Adverse Reactions")
    has_structure = False
    if ',' in text:
        # Múltiples secciones separadas por comas
        has_structure = True
    elif any(keyword in text for keyword in ['Reactions', 'Studies', 'Pharmacology', 'Usage', 'Administration', 'Warnings', 'Precautions']):
        # Una sola sección reconocible
        # Verificar que no es contenido largo (debe ser corta)
        if len(text) < 100:
            has_structure = True
    
    # NO debe ser un NDA (solo números y fechas)
    is_not_nda = not (text.replace(',', '').replace('/', '').replace(' ', '').replace('-', '').isdigit() or 
                      (len(text) < 20 and text.replace(',', '').replace('/', '').replace(' ', '').replace('-', '').isdigit()))
    
    # NO debe ser un biomarker típico (muy corto, sin comas, sin palabras clave múltiples)
    biomarkers_short = ['KRAS', 'EGFR', 'ALK', 'HER2', 'ERBB2', 'HLA-B', 'CYP2C19', 'APOE', 'GAA', 'ESR']
    is_not_biomarker = text not in biomarkers_short and not (len(text) < 30 and ',' not in text and text.upper() in [b.upper() for b in biomarkers_short])
    
    return has_keyword and has_structure and is_not_nda and is_not_biomarker


def extract_sections_from_all_columns(cells: List[str], prefer_col4: bool = True) -> str:
    """
    Extrae secciones de labeling de TODAS las columnas posibles.
    
    Debido a desplazamientos de la API de Datalab, las secciones pueden aparecer en:
    - Columna 4 (correcta - prioridad)
    - Columna 5 (desplazada - normalmente Labeling Text, pero a veces tiene secciones)
    - Columna 3 (desplazada - normalmente Biomarker, pero a veces tiene secciones)
    - Columna 0 (desplazada - normalmente NDA, pero a veces tiene secciones)
    
    Args:
        cells: Lista de celdas de la fila
        prefer_col4: Si True, prioriza col4 sobre otras columnas
    
    Retorna el texto combinado de todas las columnas que contengan secciones.
    """
    sections_parts = []
    col4_sections = None
    
    # Buscar en todas las columnas (0-5)
    for col_idx in range(min(6, len(cells))):
        cell_text = cells[col_idx].strip() if col_idx < len(cells) and cells[col_idx] else ""
        
        if is_section_text(cell_text):
            if col_idx == 4 and prefer_col4:
                # Guardar col4 por separado para priorizarla
                col4_sections = cell_text
            else:
                sections_parts.append(cell_text)
    
    # Si encontramos secciones en col4, usarla primero
    if col4_sections:
        # Combinar col4 con otras columnas encontradas
        all_sections = [col4_sections] + sections_parts
        return ', '.join(all_sections)
    elif sections_parts:
        # Solo tenemos secciones de otras columnas
        return ', '.join(sections_parts)
    
    return ""


def normalize_sections(sections_text: str) -> List[str]:
    """Normaliza el texto de secciones a una lista limpia de strings."""
    if not sections_text:
        return []
    
    sections = [s.strip() for s in sections_text.split(',') if s.strip()]
    
    seen: Set[str] = set()
    unique_sections = []
    for section in sections:
        if section and section not in seen:
            seen.add(section)
            unique_sections.append(section)
    
    return unique_sections


def parse_md_table_row(line: str) -> Optional[List[str]]:
    """Parsea una fila de tabla markdown en una lista de celdas."""
    line = line.strip()
    
    if not line.startswith('|') or not line.endswith('|'):
        return None
    
    line = line[1:-1]
    
    cells = []
    current_cell = []
    in_tag = False
    
    i = 0
    while i < len(line):
        char = line[i]
        
        if char == '<':
            in_tag = True
            current_cell.append(char)
        elif char == '>':
            in_tag = False
            current_cell.append(char)
        elif char == '|' and not in_tag:
            cells.append(''.join(current_cell).strip())
            current_cell = []
        else:
            current_cell.append(char)
        
        i += 1
    
    if current_cell:
        cells.append(''.join(current_cell).strip())
    
    while len(cells) < 6:
        cells.append('')
    
    return cells[:6]


def is_header_row(cells: List[str]) -> bool:
    """Verifica si una fila es el encabezado de la tabla."""
    if not cells or len(cells) < 2:
        return False
    
    first_cell = cells[0].lower()
    return 'nda' in first_cell or 'anda' in first_cell or 'bla' in first_cell


def is_separator_row(line: str) -> bool:
    """Verifica si una línea es el separador de tabla."""
    return re.match(r'^\|[\s\-\|]+\|$', line.strip())


def is_footnote_line(line: str) -> bool:
    """Verifica si una línea es una nota al pie."""
    line_lower = line.lower().strip()
    if line.startswith('\\') or line.startswith('*') or line.startswith('†') or line.startswith('‡'):
        return True
    if 'therapeutic areas do not necessarily reflect' in line_lower:
        return True
    if 'representative biomarkers are listed' in line_lower:
        return True
    if 'referenced figures and tables' in line_lower:
        return True
    return False


def content_belongs_to_previous_record(
    content: str,
    prev_drug: str,
    prev_biomarker: str,
    new_drug: str,
    new_biomarker: str
) -> bool:
    """
    Determina si el contenido de una fila con nuevo NDA pertenece al registro anterior.
    Esto corrige el problema de saltos de página donde el contenido sigue siendo del fármaco anterior.
    
    REGLA CRÍTICA: Solo asignar al anterior si es MUY claro que es continuación.
    Si hay duda, el contenido pertenece al nuevo registro (más seguro para RAG).
    
    Reglas estrictas:
    1. Mismo fármaco base pero diferente número (ej. Abemaciclib (1) vs (2))
    2. El contenido NO menciona el nuevo fármaco específico
    3. El contenido comienza directamente con texto que es continuación del anterior
       (ej. "Patients with..." sin contexto previo, sugiriendo que es continuación)
    4. El contenido anterior termina de forma incompleta (sugiere continuación)
    """
    if not content or not prev_drug:
        return False
    
    content_upper = content.upper()
    prev_drug_upper = prev_drug.upper()
    new_drug_upper = new_drug.upper()
    
    # Extraer nombre base del fármaco (sin número entre paréntesis)
    prev_base = prev_drug.split('(')[0].strip().upper()
    new_base = new_drug.split('(')[0].strip().upper()
    
    # Solo aplicar si es el mismo fármaco base pero diferente número
    if prev_base != new_base or prev_drug == new_drug:
        return False
    
    # Verificar que el contenido NO menciona el nuevo fármaco específico
    if new_drug_upper in content_upper[:300]:
        return False
    
    # Verificar que el contenido comienza con texto que sugiere continuación
    # (no con títulos de sección como "1 INDICATIONS", que indicarían nuevo registro)
    content_start = content_upper[:100].strip()
    if content_start.startswith(('1 INDICATIONS', '2 DOSAGE', '6 ADVERSE', '14 CLINICAL', 'VERZENIO®', 'VERZENIO (')):
        # Empieza con título de sección o nombre del fármaco -> probablemente nuevo registro
        return False
    
    # Si el contenido comienza con "Patients with..." o texto descriptivo sin contexto,
    # y menciona MONARCH, probablemente es continuación
    if content_upper.startswith('PATIENTS WITH') and 'MONARCH' in content_upper[:500]:
        return True
    
    return False


def extract_content_from_md(md_file: str) -> Dict[str, str]:
    """
    Extrae el contenido (Labeling Text) de la columna 5 del archivo Markdown.
    
    ESTRATEGIA SIMPLIFICADA:
    - Si una fila tiene NDA: es un nuevo registro
    - Si una fila no tiene NDA: pertenece al último registro procesado
    
    La estructura del MD:
    - Columna 5 contiene el Labeling Text (ahora principalmente en texto plano)
    - Puede haber filas de continuación cuando el contenido es muy largo (saltos de página)
    - Las filas de continuación tienen col1 y col3 vacías, pero col5 contiene más contenido
    
    Retorna un diccionario {composite_key: content}
    """
    with open(md_file, 'r', encoding='utf-8') as f:
        md_lines = f.readlines()
    
    content_map: Dict[str, str] = {}
    in_table = False
    current_key: Optional[str] = None
    
    for line in md_lines:
        line = line.rstrip('\n')
        
        if is_footnote_line(line):
            continue
        
        if not line.strip():
            continue
        
        if line.strip().startswith('#'):
            continue
        
        if not line.startswith('|'):
            continue
        
        if is_separator_row(line):
            continue
        
        cells = parse_md_table_row(line)
        if not cells or len(cells) < 6:
            continue
        
        if is_header_row(cells):
            in_table = True
            continue
        
        if not in_table:
            continue
        
        # Extraer contenido de columna 5
        col5_text = cells[5].strip() if len(cells) > 5 and cells[5] else ""
        content = clean_text_content(col5_text) if col5_text else ""
        
        # Buscar NDA en columnas 0-2 (misma lógica que extract_metadata_from_md)
        nda_text = None
        nda_column_idx = None
        
        for col_idx in [0, 1, 2]:
            if col_idx < len(cells):
                cell_text = cells[col_idx].strip() if cells[col_idx] else ""
                if is_nda_number(cell_text):
                    nda_text = cell_text
                    nda_column_idx = col_idx
                    break
        
        # Si tiene NDA: nuevo registro
        if nda_text:
            # Extraer metadata - usar misma lógica que extract_metadata_from_md
            nda_anda_bla_number_label_version_date = clean_text(nda_text)
            
            # Ajustar índices según dónde se encontró el NDA
            if nda_column_idx == 0:
                drug_name = clean_text(cells[1]) if len(cells) > 1 and cells[1] else ""
                biomarker = clean_text(cells[3]) if len(cells) > 3 and cells[3] else ""
            elif nda_column_idx == 1:
                drug_name = clean_text(cells[2]) if len(cells) > 2 and cells[2] else ""
                biomarker = clean_text(cells[4]) if len(cells) > 4 and cells[4] else ""
            elif nda_column_idx == 2:
                drug_name = clean_text(cells[3]) if len(cells) > 3 and cells[3] else ""
                biomarker = clean_text(cells[5]) if len(cells) > 5 and cells[5] else ""
            else:
                drug_name = clean_text(cells[1]) if len(cells) > 1 and cells[1] else ""
                biomarker = clean_text(cells[3]) if len(cells) > 3 and cells[3] else ""
            
            # Crear clave única compuesta
            current_key = create_composite_key(
                nda_anda_bla_number_label_version_date,
                drug_name,
                biomarker
            )
            
            # Inicializar contenido del nuevo registro
            if content and not is_table_header_text(content):
                content_map[current_key] = content
        
        # Si NO tiene NDA: continuación del registro actual
        elif current_key:
            if content and not is_table_header_text(content):
                if current_key in content_map:
                    content_map[current_key] += '\n\n' + content
                else:
                    content_map[current_key] = content
    
    return content_map


def extract_metadata_from_md(md_file: str) -> List[Dict]:
    """
    Extrae SOLO metadata del archivo Markdown.
    NO modifica esta función - funciona correctamente.
    
    Extrae: NDA, Drug, Area, Biomarker, Sections
    NO extrae: Content (se extrae en extract_content_from_md)
    """
    with open(md_file, 'r', encoding='utf-8') as f:
        md_lines = f.readlines()
    
    metadata_dict: Dict[str, Dict] = {}  # {composite_key: metadata}
    
    in_table = False
    current_key: Optional[str] = None  # Track del registro actual para capturar continuaciones
    
    for line in md_lines:
        line = line.rstrip('\n')
        
        if is_footnote_line(line):
            continue
        
        if not line.strip():
            continue
        
        if line.strip().startswith('#'):
            continue
        
        if not line.startswith('|'):
            continue
        
        if is_separator_row(line):
            in_table = True
            continue
        
        cells = parse_md_table_row(line)
        if not cells or len(cells) < 6:
            continue
        
        if is_header_row(cells):
            in_table = True
            continue
        
        if not in_table:
            continue
        
        # Extraer columnas
        col0_text = cells[0].strip() if cells[0] else ""
        col1_text = cells[1].strip() if len(cells) > 1 and cells[1] else ""
        col3_text = cells[3].strip() if len(cells) > 3 and cells[3] else ""
        
        # SOLUCIÓN GENÉRICA: Buscar NDA en todas las columnas cuando hay desplazamiento
        # Debido a desplazamientos de la API, el NDA puede estar en col0, col1, o incluso col2
        nda_text = None
        nda_column_idx = None
        
        # Prioridad: col0 > col1 > col2
        for col_idx in [0, 1, 2]:
            if col_idx < len(cells):
                cell_text = cells[col_idx].strip() if cells[col_idx] else ""
                if is_nda_number(cell_text):
                    nda_text = cell_text
                    nda_column_idx = col_idx
                    break
        
        # Verificar si es inicio de nuevo registro (NDA válido encontrado)
        if nda_text:
            # Extraer metadata - ajustar índices según dónde se encontró el NDA
            nda_anda_bla_number_label_version_date = clean_text(nda_text)
            
            # SOLUCIÓN GENÉRICA: Extraer drug_name, area, biomarker según desplazamiento
            # Si NDA está en col0: drug en col1, area en col2, biomarker en col3
            # Si NDA está en col1: drug en col2, area en col3, biomarker en col4
            # Si NDA está en col2: drug en col3, area en col4, biomarker en col5
            
            if nda_column_idx == 0:
                # Estructura normal
                drug_name = clean_text(cells[1]) if len(cells) > 1 and cells[1] else ""
                therapeutic_area = clean_text(cells[2]) if len(cells) > 2 and cells[2] else ""
                biomarker = clean_text(cells[3]) if len(cells) > 3 and cells[3] else ""
            elif nda_column_idx == 1:
                # Desplazado una columna a la derecha
                drug_name = clean_text(cells[2]) if len(cells) > 2 and cells[2] else ""
                therapeutic_area = clean_text(cells[3]) if len(cells) > 3 and cells[3] else ""
                biomarker = clean_text(cells[4]) if len(cells) > 4 and cells[4] else ""
            elif nda_column_idx == 2:
                # Desplazado dos columnas a la derecha
                drug_name = clean_text(cells[3]) if len(cells) > 3 and cells[3] else ""
                therapeutic_area = clean_text(cells[4]) if len(cells) > 4 and cells[4] else ""
                biomarker = clean_text(cells[5]) if len(cells) > 5 and cells[5] else ""
            else:
                # Fallback a estructura normal
                drug_name = clean_text(cells[1]) if len(cells) > 1 and cells[1] else ""
                therapeutic_area = clean_text(cells[2]) if len(cells) > 2 and cells[2] else ""
                biomarker = clean_text(cells[3]) if len(cells) > 3 and cells[3] else ""
            
            # SOLUCIÓN GENÉRICA: Buscar secciones en TODAS las columnas
            # Debido a desplazamientos de la API, pueden estar en col4, col5, col3, o col0
            labeling_sections_text = extract_sections_from_all_columns(cells)
            if not labeling_sections_text:
                # Fallback: usar col4 si no se encontró nada (compatibilidad)
                labeling_sections_text = clean_text(cells[4]) if len(cells) > 4 and cells[4] else ""
            
            labeling_sections = normalize_sections(labeling_sections_text)
            
            # Crear clave única compuesta
            new_key = create_composite_key(
                nda_anda_bla_number_label_version_date,
                drug_name,
                biomarker
            )
            
            # Guardar metadata (solo si no existe ya)
            if new_key not in metadata_dict:
                metadata_dict[new_key] = {
                    'nda_anda_bla_number_label_version_date': nda_anda_bla_number_label_version_date,
                    'drug_name': drug_name,
                    'therapeutic_area': therapeutic_area,
                    'biomarker': biomarker,
                    'labeling_sections': labeling_sections,
                    'composite_key': new_key,  # Mantener para el mapeo
                }
                current_key = new_key
            else:
                # Si ya existe, actualizar labeling_sections si hay información adicional
                current_key = new_key
                if labeling_sections_text:
                    existing_sections = metadata_dict[new_key]['labeling_sections']
                    new_sections = normalize_sections(labeling_sections_text)
                    # Combinar secciones (evitar duplicados)
                    combined = list(set(existing_sections + new_sections))
                    metadata_dict[new_key]['labeling_sections'] = combined
        elif current_key and current_key in metadata_dict:
            # Fila de continuación: verificar si tiene información adicional en labeling_sections
            # SOLUCIÓN GENÉRICA: Buscar secciones en TODAS las columnas
            col1_text = clean_text(cells[1]) if len(cells) > 1 and cells[1] else ""
            col3_text = clean_text(cells[3]) if len(cells) > 3 and cells[3] else ""
            
            # Verificar que pertenece al registro actual
            is_continuation = False
            if not col1_text and not col3_text:
                # Columnas vacías = continuación del registro actual
                is_continuation = True
            elif col1_text and col3_text:
                # Verificar si coinciden con el registro actual
                current_record = metadata_dict[current_key]
                if clean_text(col1_text) == current_record['drug_name'] and clean_text(col3_text) == current_record['biomarker']:
                    is_continuation = True
            
            if is_continuation:
                # SOLUCIÓN GENÉRICA: Extraer secciones de TODAS las columnas
                sections_to_add = extract_sections_from_all_columns(cells)
                
                if sections_to_add:
                    existing_sections = metadata_dict[current_key]['labeling_sections'].copy()
                    new_sections = normalize_sections(sections_to_add)
                    
                    # CRÍTICO: Combinar "Adverse" incompleto con "Reactions" que viene después
                    # Si existe "Adverse" y viene "Reactions", combinarlos
                    if 'Adverse' in existing_sections and 'Reactions' in new_sections:
                        # Encontrar la posición de "Adverse" para mantener el orden
                        adverse_idx = existing_sections.index('Adverse')
                        existing_sections = [s for s in existing_sections if s != 'Adverse']
                        new_sections = [s for s in new_sections if s != 'Reactions']
                        # Insertar "Adverse Reactions" en la posición original de "Adverse"
                        existing_sections.insert(adverse_idx, 'Adverse Reactions')
                    
                    # Combinar secciones preservando el orden original
                    # Agregar nuevas secciones al final, evitando duplicados
                    seen = set(existing_sections)
                    for section in new_sections:
                        if section not in seen:
                            existing_sections.append(section)
                            seen.add(section)
                    
                    metadata_dict[current_key]['labeling_sections'] = existing_sections
    
    # Convertir a lista de registros
    return list(metadata_dict.values())


# Función extract_content_from_pdf_with_marker eliminada
# Ya no se usa Marker - todo el contenido se extrae del archivo Markdown


def combine_metadata_and_content(
    metadata_records: List[Dict],
    content_map: Dict[str, str]
) -> List[Dict]:
    """
    Fase 3: Combina metadata del Markdown con contenido de la columna 5 (Labeling Text).
    Mapea usando la clave compuesta única (NDA + Drug + Biomarker).
    """
    for record in metadata_records:
        composite_key = record.get('composite_key', '')
        if composite_key and composite_key in content_map:
            record['content'] = content_map[composite_key]
        else:
            # Si no encontramos contenido, dejar vacío
            record['content'] = ""
        
        # Eliminar la clave compuesta del output final (solo era para mapeo interno)
        if 'composite_key' in record:
            del record['composite_key']
    
    return metadata_records


def process_files(md_file: str) -> List[Dict]:
    """
    Función principal: procesa el archivo Markdown para extraer metadata y contenido.
    
    Estrategia:
    1. Extrae metadata del archivo Markdown (.md) - NDA, Drug, Area, Biomarker, Sections
    2. Extrae contenido (Labeling Text) de la columna 5 del archivo Markdown
    3. Combina metadata y contenido usando la clave compuesta única.
    """
    print("Procesando archivo Markdown:")
    print(f"  Archivo: {md_file}")
    print("=" * 60)
    
    # Fase 1: Extraer metadata del Markdown
    print("\n[Fase 1] Extrayendo metadata del Markdown...")
    metadata_records = extract_metadata_from_md(md_file)
    print(f"  Encontrados {len(metadata_records)} registros de metadata")
    
    # Fase 2: Extraer contenido del Markdown (columna 5 - Labeling Text)
    print("\n[Fase 2] Extrayendo contenido del Markdown (columna 5 - Labeling Text)...")
    content_map = extract_content_from_md(md_file)
    if content_map:
        print(f"  ✓ Extraídos {len(content_map)} registros con contenido del Markdown")
    else:
        print(f"  ⚠️  No se pudo extraer contenido del Markdown")
    
    # Fase 3: Combinar metadata y contenido
    print("\n[Fase 3] Combinando metadata y contenido...")
    records = combine_metadata_and_content(metadata_records, content_map)
    
    # Estadísticas
    records_with_content = sum(1 for r in records if r.get('content'))
    print(f"  {records_with_content} de {len(records)} registros tienen contenido")
    
    return records


def main():
    """Función principal del script."""
    md_file = 'datalab-output-Biomarker Table with Text 01-06 2024 FINAL (1).pdf.md'
    output_file = 'output.json'
    
    print("=" * 60)
    
    # Usar MD para extraer tanto metadata como contenido
    records = process_files(md_file)
    
    print("=" * 60)
    print(f"\nTotal de registros generados: {len(records)}")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
    
    print(f"Archivo {output_file} generado exitosamente.")
    
    if records:
        print("\nEjemplo del primer registro:")
        example = records[0].copy()
        if len(example.get('content', '')) > 300:
            example['content'] = example['content'][:300] + "..."
        print(json.dumps(example, indent=2, ensure_ascii=False))


if __name__ == '__main__':
    main()
