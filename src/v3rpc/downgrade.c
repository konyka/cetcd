#include "cetcd/v3rpc.h"

#include <string.h>

int cetcd_v3rpc_downgrade_ok(int action, const char *version) {
    if (action != CETCD_DOWNGRADE_VALIDATE) return 0;
    if (!version || !version[0]) return 0;
    return strcmp(version, cetcd_version()) == 0;
}
