select count() as row_count
from {{ ref('wremotely__publication_manifest') }}
having row_count != 1
