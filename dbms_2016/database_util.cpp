#include "database_util.h"
#include "database_util.h"
#include <cassert>

int db::parse_int(unsigned char * const record, const table_attr_desc_t &desc)
{
	// Check type
	if (desc.type != ATTR_TYPE_INTEGER)
		throw TYPE_CAST_ERROR;

	return *(reinterpret_cast<int*>(&record[desc.offset]));
}

char * db::parse_varchar(unsigned char * const record, const table_attr_desc_t & desc)
{
	if (desc.type != ATTR_TYPE_VARCHAR)
		throw TYPE_CAST_ERROR;

	char *ret = (reinterpret_cast<char*>(&record[desc.offset]));
	if (ret == NULL)
		throw BAD_ADDR;
	return ret;
}

void db::print_record(const table_attr_desc_t * pDesc, const unsigned char * pRecord)
{
	assert(pDesc != NULL);

	int ival;
	char sval[ATTR_SIZE_MAX];
	switch (pDesc->type)
	{
	case ATTR_TYPE_INTEGER:
		memcpy(&ival, pRecord + pDesc->offset, pDesc->size);
		printf("%-*d", INT_OUTPUT_WIDTH, ival);
		break;
	case ATTR_TYPE_VARCHAR:
		memcpy(sval, pRecord + pDesc->offset, pDesc->size);
		if (sval[0] == '\0') printf("%-*s", pDesc->size, "NULL");
		else printf("%-*s",pDesc->size, sval);
		break;
	case ATTR_TYPE_UNDEFINED:
		printf("NULL");
		break;
	default:
		throw UNRESOLVED_ATTR_TYPE;
	}
}
