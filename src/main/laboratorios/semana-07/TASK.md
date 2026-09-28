<h1>Laboratorio Integrador I</h1>
<h2>Objetivo</h2>
<p>Construir una tubería ELT reproducible que ingiera, almacene, transforme y modele los datos de NYC Yellow Taxi.</p>
<h2>Datos</h2>
<p>Utilicen los datos de NYC Yellow Taxi correspondientes a:</p>
<ul>
<li>
<p>Enero–diciembre de 2025.</p>
</li>
<li>
<p>Enero–agosto de 2026.</p>
</li>
</ul>
<p>En total deben procesar 20 meses de datos.</p>
<h2>Requerimientos</h2>
<p>Levanten la infraestructura necesaria para ejecutar la tubería completa.</p>
<p>La solución debe:</p>
<ol start="1">
<li>
<p>Ingresar automáticamente los archivos de NYC Yellow Taxi.</p>
</li>
<li>
<p>Cargar los datos originales en Snowflake.</p>
</li>
<li>
<p>Implementar las transformaciones utilizando dbt.</p>
</li>
<li>
<p>Organizar los modelos utilizando una arquitectura de Bronze → Silver → Gold.</p>
</li>
</ol>
<h3>Bronze</h3>
<p>Mantener los datos lo más cercanos posible a la fuente original.</p>
<p>Agregar metadata que permita identificar:</p>
<ul>
<li>
<p>Archivo o período de origen.</p>
</li>
<li>
<p>Fecha de carga.</p>
</li>
</ul>
<h3>Silver</h3>
<p>Limpiar y estandarizar los datos aplicando las dimensiones de calidad de datos.</p>
<p>Deben tratar:</p>
<ul>
<li>
<p>Tipos de datos.</p>
</li>
<li>
<p>Valores nulos.</p>
</li>
<li>
<p>Duplicados.</p>
</li>
<li>
<p>Registros inválidos.</p>
</li>
<li>
<p>Nombres y formatos inconsistentes.</p>
</li>
</ul>
<p>Las decisiones de limpieza deben estar justificadas.</p>
<h3>Gold</h3>
<p>Construir un esquema estrella orientado al análisis de los viajes.</p>
<p>Debe existir:</p>
<ul>
<li>
<p>Una tabla de hechos.</p>
</li>
<li>
<p>Las dimensiones necesarias para analizar los viajes desde diferentes perspectivas.</p>
</li>
</ul>
<p>Definan correctamente:</p>
<ul>
<li>
<p>Grano de la tabla de hechos.</p>
</li>
<li>
<p>Primary keys de las dimensiones.</p>
</li>
<li>
<p>Foreign keys entre hechos y dimensiones.</p>
</li>
<li>
<p>Métricas y atributos.</p>
</li>
</ul>
<h2>Validación</h2>
<p>Incluyan pruebas de dbt para validar al menos:</p>
<ul>
<li>
<p><code>not_null</code></p>
</li>
<li>
<p><code>unique</code></p>
</li>
<li>
<p><code>relationships</code></p>
</li>
</ul>
<p>La tubería debe poder ejecutarse nuevamente sin generar duplicados ni inconsistencias.</p>
<h2>Entrega</h2>
<p><span style="text-decoration: underline;">En el repo de la clase, crear en la carpeta de labs la carpeta semana-07 y ahí adentro el laboratorio; subir solo el link de su github.</span></p>
<p>Entreguen:</p>
<ul>
<li>
<p>Código de infraestructura.</p>
</li>
<li>
<p>Código de ingesta.</p>
</li>
<li>
<p>Proyecto dbt.</p>
</li>
<li>
<p>Diagrama de la arquitectura.</p>
</li>
<li>
<p>Diagrama del esquema estrella.</p>
</li>
<li>
<p>README con instrucciones para levantar y ejecutar la solución.</p>
</li>
</ul>
<p>Al finalizar, debe ser posible ejecutar el pipeline desde cero y obtener en Snowflake las capas Bronze, Silver y Gold listas para el análisis.<br><br><span style="text-decoration: underline;">Apóyense en la IA para resolver este laboratorio.</span></p></div>
			<!--?lit$147713113$-->
		</template></d2l-html-block></div>