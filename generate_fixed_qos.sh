#!/bin/bash
# 这个脚本用于生成修正后的 init_slurm_qos.sh 代码 (V39 - Explicit Account-Partition Associations)

cat << 'EOF'
#!/bin/bash
# =================================================================
# 脚本名称: init_slurm_qos.sh (V39 - Explicit Account-Partition Associations)
# =================================================================

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
CONFIG_FILE="${SCRIPT_DIR}/slurm_limits.csv"
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'

[[ ! -f "$CONFIG_FILE" ]] && { echo -e "${RED}错误: 找不到配置文件 $CONFIG_FILE${NC}"; exit 1; }

# 获取集群名称 (sacctmgr 操作需要)
CLUSTER=$(sacctmgr -n -p show cluster format=Cluster | head -n 1 | cut -d'|' -f1)
[[ -z "$CLUSTER" ]] && CLUSTER="sycamore"

echo -e "${GREEN}[$(date)] 开始同步 Slurm 体系 (V39) [Cluster: $CLUSTER]...${NC}"

# 1. 第一步：全局 QOS 基础配置
sacctmgr -i modify qos where name=normal set Priority=100 Flags=DenyOnLimit >/dev/null 2>&1
sacctmgr -i modify qos where name=webapp set Priority=50 Flags=DenyOnLimit >/dev/null 2>&1
sacctmgr -i modify qos where name=debug set Flags=DenyOnLimit,OverPartQOS >/dev/null 2>&1

ALL_ACCS=$(sacctmgr -n -p show account format=Account | cut -d'|' -f1 | grep -E '^(lab-|ext-)' | sort -u)

# 2. 第二步：遍历 CSV 并显式应用限制
grep -v '^#' "$CONFIG_FILE" | grep -v '^$' | while IFS=, read -r C_ACC C_PART C_QOS C_DEFWALL C_MAXWALL C_NODES C_TRES C_MAXJOBS C_GRPJOBS C_DESC
do
    ACC_RAW=$(echo "$C_ACC" | xargs); PART=$(echo "$C_PART" | xargs); QOS=$(echo "$C_QOS" | xargs)
    MAXJOBS=$(echo "$C_MAXJOBS" | xargs); GRPJOBS=$(echo "$C_GRPJOBS" | xargs); NODES=$(echo "$C_NODES" | xargs)
    
    echo -e "${YELLOW}应用配置: 分区=${PART}, QOS=${QOS}, 个人限制=${MAXJOBS}, 组限制=${GRPJOBS}${NC}"
    
    # 清理 TRES 字符串
    CLEAN_TRES=$(echo "$C_TRES" | tr ';' ',' | xargs)
    [[ "$PART" == *"4090"* ]] && CLEAN_TRES=$(echo "$CLEAN_TRES" | sed 's/gres\/gpu=/gres\/gpu:4090=/')
    [[ "$PART" == *"h100"* ]] && CLEAN_TRES=$(echo "$CLEAN_TRES" | sed 's/gres\/gpu=/gres\/gpu:h100=/')

    [[ "$ACC_RAW" == "root" ]] && TARGET_ACCS="$ALL_ACCS" || TARGET_ACCS="$ACC_RAW"

    for ACC in $TARGET_ACCS; do
        # A. 设置账号在特定分区下的组限制 (Account-Partition Association)
        if [[ "$QOS" == "normal" ]]; then
            SHARED_TRES=$(echo "$CLEAN_TRES" | perl -pe 's/(\d+)/$1*2/ge')
            
            # 重要：确保账号的分区关联存在 (这是设置账号分区级限制的前提)
            sacctmgr -i add association account="$ACC" cluster="$CLUSTER" partition="$PART" qos="$QOS" >/dev/null 2>&1
            
            # 明确修改该关联的 GrpJobs
            sacctmgr -i modify association where account="$ACC" cluster="$CLUSTER" partition="$PART" and user="" set GrpJobs=$GRPJOBS GrpNodes=$((NODES * 2)) GrpTRES="$SHARED_TRES" >/dev/null 2>&1
            
            # 同时清除该账号的“全局/无分区”限制，防止干扰
            sacctmgr -i modify association where account="$ACC" cluster="$CLUSTER" partition="" and user="" set GrpJobs=-1 >/dev/null 2>&1
        fi

        # B. 遍历并设置用户层级限制
        USERS=$(sacctmgr -n -p show assoc account="$ACC" format=User | cut -d'|' -f1 | grep -vE '^$|root' | sort -u)
        for USER in $USERS; do
            # 建立用户在该分区下的关联
            sacctmgr -i add user "$USER" account="$ACC" partition="$PART" cluster="$CLUSTER" >/dev/null 2>&1
            sacctmgr -i modify association where user="$USER" account="$ACC" partition="$PART" set QOS+="$QOS" >/dev/null 2>&1
            
            # C. 显式设置个人的并发限制
            if [[ "$QOS" == "normal" ]]; then
                sacctmgr -i modify association where user="$USER" account="$ACC" partition="$PART" set MaxJobs=$MAXJOBS MaxSubmitJobs=$((MAXJOBS * 2)) GrpJobs=-1 GrpTRES="$CLEAN_TRES" >/dev/null 2>&1
            fi
        done
    done
done

scontrol reconfigure
echo -e "${GREEN}同步完成。请检查限制是否已生效。${NC}"
EOF
