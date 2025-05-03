package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "net/http/pprof" // Add this line to import pprof

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/fatih/color"
	"github.com/gorilla/mux"
)

// Configuration represents the application configuration
type Configuration struct {
	RPCEndpoints    []string             `json:"rpcEndpoints"`
	WSEndpoints     []string             `json:"wsEndpoints"`
	PrivateKeys     []string             `json:"privateKeys"`
	ContractAddress string               `json:"contractAddress"`
	TokenAddresses  map[string]Token     `json:"tokenAddresses"`
	DEXes           map[string]DEXConfig `json:"dexes"`
	MinProfit       string               `json:"minProfit"`
	GasSettings     GasSettings          `json:"gasSettings"`
	APIPort         string               `json:"apiPort"`
	TokenPairs      []TokenPair          `json:"tokenPairs"`
	DefaultLender   string               `json:"defaultLender"` // "aave" or "balancer"
	LenderConfig    LenderConfig         `json:"lenderConfig"`
}

// LenderConfig contains configuration for lending protocols
type LenderConfig struct {
	AavePoolAddress      common.Address `json:"aavePoolAddress"`
	BalancerVaultAddress common.Address `json:"balancerVaultAddress"`
}

// DEXConfig represents a DEX configuration
type DEXConfig struct {
	Name           string         `json:"name"`
	Version        string         `json:"version"` // "v2" or "v3"
	RouterAddress  common.Address `json:"routerAddress"`
	FactoryAddress common.Address `json:"factoryAddress,omitempty"` // Optional, can be fetched from router
	FeePercent     uint32         `json:"feePercent"`               // 3000 = 0.3%
}

// GasSettings contains gas-related configuration
type GasSettings struct {
	MaxGasPrice        string  `json:"maxGasPrice"`
	PriorityFee        string  `json:"priorityFee"`
	GasLimitMultiplier float64 `json:"gasLimitMultiplier"`
	SpeedUpThreshold   int     `json:"speedUpThreshold"`
}

// Token represents an ERC20 token
type Token struct {
	Symbol   string         `json:"symbol"`
	Address  common.Address `json:"address"`
	Decimals uint8          `json:"decimals"`
}

// TokenPair represents a pair of tokens to monitor
type TokenPair struct {
	TokenA           string   `json:"tokenA"`
	TokenB           string   `json:"tokenB"`
	PairKey          string   `json:"pairKey"`
	Active           bool     `json:"active"`
	DEXes            []string `json:"dexes"`            // List of DEX names to monitor for this pair
	LenderPreference string   `json:"lenderPreference"` // "aave", "balancer", or "" for default
}

// BotInstance represents a single bot monitoring a token pair
type BotInstance struct {
	TokenPair *TokenPair
	Prices    map[string]*big.Float // DEX name -> Price
	LastCheck time.Time
	Status    string // "running", "stopped", "error"
	Error     string // Last error message
}

type ExecutionResult struct {
	Opportunity    *ArbitrageOpportunity
	Success        bool
	ExecutionTime  time.Duration
	TxHash         common.Hash
	GasUsed        uint64
	ActualProfit   *big.Float
	Error          string
	Timestamp      time.Time
	SimulationOnly bool
}

// TradeExecutor handles the execution of arbitrage opportunities
type TradeExecutor struct {
	config           *Configuration
	logger           *Logger
	client           *ethclient.Client
	activeExecutions sync.Map
	executionResults []*ExecutionResult
	resultsMutex     sync.RWMutex
	simulationMode   bool
}

// NewTradeExecutor creates a new instance of TradeExecutor
func NewTradeExecutor(config *Configuration, logger *Logger, client *ethclient.Client) *TradeExecutor {
	return &TradeExecutor{
		config:           config,
		logger:           logger,
		client:           client,
		executionResults: make([]*ExecutionResult, 0),
		simulationMode:   true, // Start in simulation mode by default
	}
}

// ArbitrageOpportunity represents a potential arbitrage opportunity
type ArbitrageOpportunity struct {
	Timestamp         time.Time  `json:"timestamp"`
	PairKey           string     `json:"pairKey"`
	BuyDEX            string     `json:"buyDEX"`
	SellDEX           string     `json:"sellDEX"`
	BuyDEXVersion     string     `json:"buyDEXVersion"`
	SellDEXVersion    string     `json:"sellDEXVersion"`
	BuyPrice          *big.Float `json:"buyPrice"`
	SellPrice         *big.Float `json:"sellPrice"`
	PriceGap          *big.Float `json:"priceGap"`
	ProfitPercent     *big.Float `json:"profitPercent"`
	GasCost           *big.Float `json:"gasCost"`
	EstimatedProfit   *big.Float `json:"estimatedProfit"`
	ProfitInUSD       *big.Float `json:"profitInUSD"`
	ProfitInToken     *big.Float `json:"profitInToken"`
	TradeAmountUSD    *big.Float `json:"tradeAmountUSD"`
	TradeAmountToken  *big.Float `json:"tradeAmountToken"`
	IsViable          bool       `json:"isViable"`
	LenderUsed        string     `json:"lenderUsed"`
	ReserveA          *big.Int   `json:"reserveA"`
	ReserveB          *big.Int   `json:"reserveB"`
	RecommendedAmount *big.Float `json:"recommendedAmount"`

	// New flash loan fields
	FlashLoanAmount    *big.Float `json:"flashLoanAmount"`
	FlashLoanProfitUSD *big.Float `json:"flashLoanProfitUSD"`
	FlashLoanValueUSD  *big.Float `json:"flashLoanValueUSD"`
	IsFlashLoanViable  bool       `json:"isFlashLoanViable"`
}

// ABIs for various contracts
const (
	// ERC20 ABI with just the methods we need
	ERC20ABI = `[
		{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},
		{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"}
	]`

	// Router ABI with just the factory method
	RouterABIV2 = `[
		{"constant":true,"inputs":[],"name":"factory","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"}
	]`

	// Factory ABI with just the getPair method
	FactoryABIV2 = `[
		{"constant":true,"inputs":[{"name":"tokenA","type":"address"},{"name":"tokenB","type":"address"}],"name":"getPair","outputs":[{"name":"pair","type":"address"}],"payable":false,"stateMutability":"view","type":"function"}
	]`

	// Pair ABI with just the methods we need
	PairABIV2 = `[
		{"constant":true,"inputs":[],"name":"token0","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},
		{"constant":true,"inputs":[],"name":"token1","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},
		{"constant":true,"inputs":[],"name":"getReserves","outputs":[{"name":"reserve0","type":"uint112"},{"name":"reserve1","type":"uint112"},{"name":"blockTimestampLast","type":"uint32"}],"payable":false,"stateMutability":"view","type":"function"}
	]`

	// UniswapV3 Factory ABI (simplified)
	FactoryABIV3 = `[
		{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"},{"internalType":"uint24","name":"fee","type":"uint24"}],"name":"getPool","outputs":[{"internalType":"address","name":"pool","type":"address"}],"stateMutability":"view","type":"function"}
	]`

	// UniswapV3 Pool ABI (simplified)
	PoolABIV3 = `[
		{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"observe","outputs":[{"internalType":"int56[]","name":"tickCumulatives","type":"int56[]"},{"internalType":"uint160[]","name":"secondsPerLiquidityCumulativeX128s","type":"uint160[]"}],"stateMutability":"view","type":"function"},
		{"inputs":[],"name":"slot0","outputs":[{"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"internalType":"int24","name":"tick","type":"int24"},{"internalType":"uint16","name":"observationIndex","type":"uint16"},{"internalType":"uint16","name":"observationCardinality","type":"uint16"},{"internalType":"uint16","name":"observationCardinalityNext","type":"uint16"},{"internalType":"uint8","name":"feeProtocol","type":"uint8"},{"internalType":"bool","name":"unlocked","type":"bool"}],"stateMutability":"view","type":"function"},
		{"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},
		{"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}
	]`

	AaveV3PoolABI = `[
        {"inputs":[{"internalType":"address","name":"receiverAddress","type":"address"},{"internalType":"address[]","name":"assets","type":"address[]"},{"internalType":"uint256[]","name":"amounts","type":"uint256[]"},{"internalType":"uint256[]","name":"interestRateModes","type":"uint256[]"},{"internalType":"address","name":"onBehalfOf","type":"address"},{"internalType":"bytes","name":"params","type":"bytes"},{"internalType":"uint16","name":"referralCode","type":"uint16"}],"name":"flashLoan","outputs":[],"stateMutability":"nonpayable","type":"function"},
        {"inputs":[{"internalType":"address","name":"asset","type":"address"}],"name":"getReserveData","outputs":[{"components":[{"components":[{"internalType":"uint256","name":"data","type":"uint256"}],"internalType":"struct DataTypes.ReserveConfigurationMap","name":"configuration","type":"tuple"},{"internalType":"uint128","name":"liquidityIndex","type":"uint128"},{"internalType":"uint128","name":"currentLiquidityRate","type":"uint128"},{"internalType":"uint128","name":"variableBorrowIndex","type":"uint128"},{"internalType":"uint128","name":"currentVariableBorrowRate","type":"uint128"},{"internalType":"uint128","name":"currentStableBorrowRate","type":"uint128"},{"internalType":"uint40","name":"lastUpdateTimestamp","type":"uint40"},{"internalType":"uint16","name":"id","type":"uint16"},{"internalType":"address","name":"aTokenAddress","type":"address"},{"internalType":"address","name":"stableDebtTokenAddress","type":"address"},{"internalType":"address","name":"variableDebtTokenAddress","type":"address"},{"internalType":"address","name":"interestRateStrategyAddress","type":"address"},{"internalType":"uint128","name":"accruedToTreasury","type":"uint128"},{"internalType":"uint128","name":"unbacked","type":"uint128"},{"internalType":"uint128","name":"isolationModeTotalDebt","type":"uint128"}],"internalType":"struct DataTypes.ReserveData","name":"","type":"tuple"}],"stateMutability":"view","type":"function"}
    ]`

	// Aave V3 Flash Loan Receiver Interface
	AaveV3FlashLoanReceiverABI = `[
        {"inputs":[{"internalType":"address[]","name":"assets","type":"address[]"},{"internalType":"uint256[]","name":"amounts","type":"uint256[]"},{"internalType":"uint256[]","name":"premiums","type":"uint256[]"},{"internalType":"address","name":"initiator","type":"address"},{"internalType":"bytes","name":"params","type":"bytes"}],"name":"executeOperation","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"}
    ]`
)

// DEX represents a decentralized exchange
type DEX struct {
	Config        DEXConfig
	client        *ethclient.Client
	logger        *Logger
	routerABI     abi.ABI
	factoryABI    abi.ABI
	pairOrPoolABI abi.ABI
	erc20ABI      abi.ABI
}

// Logger provides color-coded logging
type Logger struct {
	mu sync.Mutex
}

func NewLogger() *Logger {
	return &Logger{}
}

func (l *Logger) Info(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	color.Cyan("[INFO] "+format, args...)
}

func (l *Logger) Error(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	color.Red("[ERROR] "+format, args...)
}

func (l *Logger) Warning(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	color.Yellow("[WARNING] "+format, args...)
}

func (l *Logger) Success(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	color.Green("[SUCCESS] "+format, args...)
}

func (l *Logger) Price(dex, pair string, price *big.Float) {
	l.mu.Lock()
	defer l.mu.Unlock()
	priceStr := price.Text('f', 8)
	color.Magenta("[PRICE] %s on %s: %s", pair, dex, priceStr)
}

func (l *Logger) Opportunity(opp *ArbitrageOpportunity) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Format profit with appropriate scaling based on token type
	var profitStr string
	if opp.ProfitInUSD != nil {
		profit, _ := opp.ProfitInUSD.Float64()
		profitStr = fmt.Sprintf("$%.2f", profit)
	} else {
		// Fallback to raw profit
		profitStr = opp.EstimatedProfit.Text('f', 4)
	}

	// percentStr := opp.ProfitPercent.Text('f', 2)
	buyPriceStr := opp.BuyPrice.Text('f', 6)
	sellPriceStr := opp.SellPrice.Text('f', 6)

	// Convert percentage to float for easier comparison
	percentFloat, _ := opp.ProfitPercent.Float64()

	// Consider any opportunity with ≥1% profit as viable regardless of other factors
	isHighProfit := percentFloat >= 1.0

	if opp.IsViable || isHighProfit {
		color.New(color.FgHiWhite, color.BgGreen).Printf(
			"[VIABLE OPPORTUNITY] %s: Buy on %s (%s) at %s, sell on %s (%s) at %s. Profit: %s (%.2f%%) 🚀\n",
			opp.PairKey, opp.BuyDEX, opp.BuyDEXVersion, buyPriceStr,
			opp.SellDEX, opp.SellDEXVersion, sellPriceStr, profitStr, percentFloat,
		)
	} else {
		color.New(color.FgHiWhite, color.BgYellow).Printf(
			"[LOW OPPORTUNITY] %s: Buy on %s (%s) at %s, sell on %s (%s) at %s. Profit: %s (%.2f%%) - Below threshold\n",
			opp.PairKey, opp.BuyDEX, opp.BuyDEXVersion, buyPriceStr,
			opp.SellDEX, opp.SellDEXVersion, sellPriceStr, profitStr, percentFloat,
		)
	}
}

// MonitorService manages the monitoring of token pairs
type MonitorService struct {
	config        *Configuration
	client        *ethclient.Client
	dexes         map[string]*DEX
	logger        *Logger
	tokenPairs    map[string]*TokenPair
	pairPrices    map[string]map[string]*big.Float
	opportunities []*ArbitrageOpportunity
	ctx           context.Context
	cancel        context.CancelFunc
	pairMutex     sync.RWMutex
	priceMutex    sync.RWMutex
	opportunityMu sync.RWMutex

	// Bot instances
	bots      map[string]*BotInstance
	botsMutex sync.RWMutex

	// Worker management
	workerWg    sync.WaitGroup
	pairWorkers map[string]context.CancelFunc
	workerMutex sync.Mutex
	executor    *TradeExecutor
	// Config file path
	configPath string
}

// NewMonitorService creates a new monitoring service
func NewMonitorService(configPath string) (*MonitorService, error) {
	// Load configuration
	config, err := loadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration: %w", err)
	}

	// Create context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())

	// Setup logger
	logger := NewLogger()

	// Connect to Ethereum node
	logger.Info("Connecting to Ethereum node: %s", config.RPCEndpoints[0])
	client, err := ethclient.Dial(config.RPCEndpoints[0])
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Ethereum node: %w", err)
	}

	// Create DEX instances
	dexes, err := createDEXInstances(config.DEXes, client, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create DEX instances: %w", err)
	}

	// Initialize token pairs
	tokenPairs := make(map[string]*TokenPair)
	for _, pair := range config.TokenPairs {
		if pair.PairKey == "" {
			pair.PairKey = fmt.Sprintf("%s-%s", pair.TokenA, pair.TokenB)
		}
		if len(pair.DEXes) == 0 {
			// If no DEXes specified, use all available DEXes
			for dexName := range config.DEXes {
				pair.DEXes = append(pair.DEXes, dexName)
			}
		}
		pair.Active = true
		tokenPairs[pair.PairKey] = &pair
	}

	// Initialize bot instances
	bots := make(map[string]*BotInstance)
	for pairKey, pair := range tokenPairs {
		bots[pairKey] = &BotInstance{
			TokenPair: pair,
			Prices:    make(map[string]*big.Float),
			Status:    "stopped",
		}
	}

	executor := NewTradeExecutor(config, logger, client)

	return &MonitorService{
		config:        config,
		client:        client,
		dexes:         dexes,
		logger:        logger,
		tokenPairs:    tokenPairs,
		pairPrices:    make(map[string]map[string]*big.Float),
		opportunities: make([]*ArbitrageOpportunity, 0),
		ctx:           ctx,
		cancel:        cancel,
		pairWorkers:   make(map[string]context.CancelFunc),
		bots:          bots,
		configPath:    configPath,
		executor:      executor,
	}, nil
}

// createDEXInstances creates DEX instances from configuration
func createDEXInstances(dexConfigs map[string]DEXConfig, client *ethclient.Client, logger *Logger) (map[string]*DEX, error) {
	dexes := make(map[string]*DEX)

	// Parse ABIs
	erc20ABI, err := abi.JSON(strings.NewReader(ERC20ABI))
	if err != nil {
		return nil, fmt.Errorf("failed to parse ERC20 ABI: %w", err)
	}

	// Create DEX instances
	for name, config := range dexConfigs {
		var dex *DEX
		var routerABI, factoryABI, pairOrPoolABI abi.ABI

		if config.Version == "v2" {
			// Parse V2 ABIs
			routerABI, err = abi.JSON(strings.NewReader(RouterABIV2))
			if err != nil {
				return nil, fmt.Errorf("failed to parse V2 router ABI: %w", err)
			}

			factoryABI, err = abi.JSON(strings.NewReader(FactoryABIV2))
			if err != nil {
				return nil, fmt.Errorf("failed to parse V2 factory ABI: %w", err)
			}

			pairOrPoolABI, err = abi.JSON(strings.NewReader(PairABIV2))
			if err != nil {
				return nil, fmt.Errorf("failed to parse V2 pair ABI: %w", err)
			}
		} else if config.Version == "v3" {
			// Parse V3 ABIs
			// For V3 we don't use the router ABI for price checks, so just use V2 for simplicity
			routerABI, err = abi.JSON(strings.NewReader(RouterABIV2))
			if err != nil {
				return nil, fmt.Errorf("failed to parse router ABI: %w", err)
			}

			factoryABI, err = abi.JSON(strings.NewReader(FactoryABIV3))
			if err != nil {
				return nil, fmt.Errorf("failed to parse V3 factory ABI: %w", err)
			}

			pairOrPoolABI, err = abi.JSON(strings.NewReader(PoolABIV3))
			if err != nil {
				return nil, fmt.Errorf("failed to parse V3 pool ABI: %w", err)
			}
		} else {
			return nil, fmt.Errorf("unsupported DEX version: %s", config.Version)
		}

		dex = &DEX{
			Config:        config,
			client:        client,
			logger:        logger,
			routerABI:     routerABI,
			factoryABI:    factoryABI,
			pairOrPoolABI: pairOrPoolABI,
			erc20ABI:      erc20ABI,
		}

		dexes[name] = dex
		logger.Success("Created DEX %s (%s) with router %s", name, config.Version, config.RouterAddress.Hex())
	}

	return dexes, nil
}

// Start begins the monitoring service
func (m *MonitorService) Start() {
	m.logger.Info("Starting DEX Price Monitor")

	// Start monitoring each token pair
	for pairKey, pair := range m.tokenPairs {
		if pair.Active {
			m.startMonitoringPair(pairKey, pair)
		}
	}

	// Start the API server
	go m.startAPIServer()

	// Save opportunities periodically
	go m.periodicTasks()

	// Add this near the beginning of your main function or where you start your service
	go func() {
		m.logger.Info("Starting pprof server on port 6060")
		err := http.ListenAndServe("localhost:6060", nil)
		if err != nil {
			m.logger.Error("Failed to start pprof server: %v", err)
		}
	}()
}

// periodicTasks runs tasks that need to be performed periodically
func (m *MonitorService) periodicTasks() {
	// Save opportunities every 5 minutes
	opportunitiesTicker := time.NewTicker(5 * time.Minute)

	// Save config every 10 minutes
	configTicker := time.NewTicker(10 * time.Minute)

	// Clean up old opportunities every hour
	cleanupTicker := time.NewTicker(1 * time.Hour)

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-opportunitiesTicker.C:
			m.saveOpportunities()
		case <-configTicker.C:
			m.saveConfig()
		case <-cleanupTicker.C:
			m.cleanupOldOpportunities()
		}
	}
}

// cleanupOldOpportunities removes opportunities older than 24 hours
func (m *MonitorService) cleanupOldOpportunities() {
	m.opportunityMu.Lock()
	defer m.opportunityMu.Unlock()

	cutoff := time.Now().Add(-24 * time.Hour)
	var newOpportunities []*ArbitrageOpportunity

	for _, opp := range m.opportunities {
		if opp.Timestamp.After(cutoff) {
			newOpportunities = append(newOpportunities, opp)
		}
	}

	m.logger.Info("Cleaned up opportunities: %d -> %d", len(m.opportunities), len(newOpportunities))
	m.opportunities = newOpportunities
}

// Stop stops the monitoring service
func (m *MonitorService) Stop() {
	m.logger.Info("Stopping DEX Price Monitor")

	// Cancel the main context
	m.cancel()

	// Wait for all workers to stop
	m.workerWg.Wait()

	// Save final state
	m.saveOpportunities()
	m.saveConfig()

	m.logger.Success("Monitor service stopped successfully")
}

// startMonitoringPair starts monitoring a specific token pair
func (m *MonitorService) startMonitoringPair(pairKey string, pair *TokenPair) {
	m.workerMutex.Lock()
	defer m.workerMutex.Unlock()

	// Check if this pair is already being monitored
	if _, exists := m.pairWorkers[pairKey]; exists {
		m.logger.Warning("Pair %s is already being monitored", pairKey)
		return
	}

	// Create a new context for this worker
	workerCtx, workerCancel := context.WithCancel(m.ctx)
	m.pairWorkers[pairKey] = workerCancel

	m.logger.Info("Starting monitoring for pair: %s", pairKey)

	// Update bot status
	m.botsMutex.Lock()
	if bot, exists := m.bots[pairKey]; exists {
		bot.Status = "running"
		bot.LastCheck = time.Now()
	} else {
		m.bots[pairKey] = &BotInstance{
			TokenPair: pair,
			Prices:    make(map[string]*big.Float),
			Status:    "running",
			LastCheck: time.Now(),
		}
	}
	m.botsMutex.Unlock()

	// Start a goroutine to monitor this pair
	m.workerWg.Add(1)
	go func(ctx context.Context, pk string, p *TokenPair) {
		defer m.workerWg.Done()

		// Initialize price map for this pair
		m.priceMutex.Lock()
		if _, exists := m.pairPrices[pk]; !exists {
			m.pairPrices[pk] = make(map[string]*big.Float)
		}
		m.priceMutex.Unlock()

		// Create ticker for periodic checks
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		// Run once immediately
		m.checkPrices(ctx, pk, p)

		// Then on ticker interval
		for {
			select {
			case <-ctx.Done():
				m.logger.Info("Stopping monitoring for pair: %s", pk)

				// Update bot status
				m.botsMutex.Lock()
				if bot, exists := m.bots[pk]; exists {
					bot.Status = "stopped"
				}
				m.botsMutex.Unlock()

				return
			case <-ticker.C:
				m.checkPrices(ctx, pk, p)
			}
		}
	}(workerCtx, pairKey, pair)
}

// stopMonitoringPair stops monitoring a specific token pair
func (m *MonitorService) stopMonitoringPair(pairKey string) {
	m.workerMutex.Lock()
	defer m.workerMutex.Unlock()

	// Check if this pair is being monitored
	cancelFunc, exists := m.pairWorkers[pairKey]
	if !exists {
		m.logger.Warning("Pair %s is not being monitored", pairKey)
		return
	}

	// Cancel the worker context
	cancelFunc()

	// Remove from workers map
	delete(m.pairWorkers, pairKey)

	m.logger.Info("Stopped monitoring for pair: %s", pairKey)

	// Update token pair status
	m.pairMutex.Lock()
	if pair, exists := m.tokenPairs[pairKey]; exists {
		pair.Active = false
	}
	m.pairMutex.Unlock()
}

// checkPrices checks prices for a token pair across all configured DEXes
func (m *MonitorService) checkPrices(ctx context.Context, pairKey string, pair *TokenPair) {
	// Get token addresses
	tokenASymbol := pair.TokenA
	tokenBSymbol := pair.TokenB

	var tokenA, tokenB Token
	var exists bool

	tokenA, exists = m.config.TokenAddresses[tokenASymbol]
	if !exists {
		m.logger.Error("Token %s not found in configuration", tokenASymbol)
		return
	}

	tokenB, exists = m.config.TokenAddresses[tokenBSymbol]
	if !exists {
		m.logger.Error("Token %s not found in configuration", tokenBSymbol)
		return
	}

	// Update bot's last check time
	m.botsMutex.Lock()
	if bot, exists := m.bots[pairKey]; exists {
		bot.LastCheck = time.Now()
	}
	m.botsMutex.Unlock()

	// Check prices on each DEX specified for this pair
	prices := make(map[string]*big.Float)
	dexInfos := make(map[string]struct {
		version  string
		price    *big.Float
		reserveA *big.Int
		reserveB *big.Int
	})

	for _, dexName := range pair.DEXes {
		dex, exists := m.dexes[dexName]
		if !exists {
			m.logger.Error("DEX %s not found", dexName)
			continue
		}

		// Create timeout context for this check
		checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		// Get price based on DEX version
		var price *big.Float
		var reserveA, reserveB *big.Int
		var err error

		if dex.Config.Version == "v2" {
			price, reserveA, reserveB, err = m.getV2Price(checkCtx, dex, tokenA.Address, tokenB.Address, tokenA.Decimals, tokenB.Decimals)
		} else if dex.Config.Version == "v3" {
			price, err = m.getV3Price(checkCtx, dex, tokenA.Address, tokenB.Address, tokenA.Decimals, tokenB.Decimals)
			// V3 doesn't use the same concept of reserves, but we can estimate later if needed
			reserveA = big.NewInt(0)
			reserveB = big.NewInt(0)
		} else {
			m.logger.Error("Unsupported DEX version for %s: %s", dexName, dex.Config.Version)
			continue
		}

		if err != nil {
			m.logger.Error("Failed to get price for %s on %s: %v", pairKey, dexName, err)

			// Update bot status with error
			m.botsMutex.Lock()
			if bot, exists := m.bots[pairKey]; exists {
				bot.Error = err.Error()
			}
			m.botsMutex.Unlock()

			continue
		}

		// Store price
		prices[dexName] = price
		dexInfos[dexName] = struct {
			version  string
			price    *big.Float
			reserveA *big.Int
			reserveB *big.Int
		}{
			version:  dex.Config.Version,
			price:    price,
			reserveA: reserveA,
			reserveB: reserveB,
		}

		m.logger.Price(dexName, pairKey, price)

		// Update bot's prices
		m.botsMutex.Lock()
		if bot, exists := m.bots[pairKey]; exists {
			bot.Prices[dexName] = price
			bot.Error = ""
		}
		m.botsMutex.Unlock()
	}

	// Update price cache
	m.priceMutex.Lock()
	m.pairPrices[pairKey] = prices
	m.priceMutex.Unlock()

	// Check for arbitrage opportunities
	if len(prices) >= 2 {
		m.detectArbitrageOpportunities(pairKey, dexInfos, tokenA, tokenB, pair.LenderPreference)
	}
}

// getV2Price gets the price from a V2 DEX
func (m *MonitorService) getV2Price(ctx context.Context, dex *DEX, tokenAAddr, tokenBAddr common.Address, tokenADecimals, tokenBDecimals uint8) (*big.Float, *big.Int, *big.Int, error) {
	// Get factory address if not already set
	factoryAddr := dex.Config.FactoryAddress
	if factoryAddr == (common.Address{}) {
		var err error
		factoryAddr, err = m.getFactoryAddress(ctx, dex)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to get factory address: %w", err)
		}

		// Update config for future use
		dex.Config.FactoryAddress = factoryAddr
	}

	// Get pair address
	pairAddr, err := m.getPairAddress(ctx, dex, factoryAddr, tokenAAddr, tokenBAddr)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get pair address: %w", err)
	}

	if pairAddr == (common.Address{}) {
		return nil, nil, nil, fmt.Errorf("pair does not exist for tokens")
	}

	// Get token ordering in the pair
	token0Addr, err := m.getToken0(ctx, dex, pairAddr)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get token0: %w", err)
	}

	// Get reserves
	reserve0, reserve1, err := m.getReserves(ctx, dex, pairAddr)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get reserves: %w", err)
	}

	// Determine reserves based on token order
	var reserveA, reserveB *big.Int
	if token0Addr == tokenAAddr {
		reserveA = reserve0
		reserveB = reserve1
	} else {
		reserveA = reserve1
		reserveB = reserve0
	}

	// Convert reserves to big.Float for division
	reserveAFloat := new(big.Float).SetInt(reserveA)
	reserveBFloat := new(big.Float).SetInt(reserveB)

	// Calculate price (reserveB / reserveA) with decimal adjustment
	price := new(big.Float).Quo(reserveBFloat, reserveAFloat)

	// Adjust for decimals
	if tokenADecimals != tokenBDecimals {
		// Calculate decimal adjustment factor: 10^(tokenADecimals - tokenBDecimals)
		decimalAdjustment := new(big.Float)
		if tokenADecimals > tokenBDecimals {
			// Need to multiply price
			diff := tokenADecimals - tokenBDecimals
			decimalAdjustment.SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(diff)), nil))
			price.Mul(price, decimalAdjustment)
		} else {
			// Need to divide price
			diff := tokenBDecimals - tokenADecimals
			decimalAdjustment.SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(diff)), nil))
			price.Quo(price, decimalAdjustment)
		}
	}

	return price, reserveA, reserveB, nil
}

// getV3Price gets the price from a V3 DEX (simplified implementation)
func (m *MonitorService) getV3Price(ctx context.Context, dex *DEX, tokenAAddr, tokenBAddr common.Address, tokenADecimals, tokenBDecimals uint8) (*big.Float, error) {
	// For this placeholder implementation, we're not fully implementing V3 math
	// In a real implementation, this would get the sqrtPrice from the pool and convert it correctly

	// For now, generate a simulated price based on the token addresses
	// This is just a placeholder - the real implementation would use real contract calls
	seed := time.Now().UnixNano() % 1000
	basePrice := 1.0 + float64(seed)/10000.0

	// In reality, this should call V3 pool's slot0() function and calculate price using sqrt math
	return big.NewFloat(basePrice), nil
}

// getFactoryAddress gets the factory address from a router
func (m *MonitorService) getFactoryAddress(ctx context.Context, dex *DEX) (common.Address, error) {
	// Use simulated value if enabled
	if os.Getenv("SIMULATION_MODE") == "1" {
		return common.HexToAddress("0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"), nil
	}

	// Call the router's factory() function
	data, err := dex.CallContract(ctx, dex.Config.RouterAddress, dex.routerABI, "factory")
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to call factory(): %w", err)
	}

	var result common.Address
	if err := dex.routerABI.UnpackIntoInterface(&result, "factory", data); err != nil {
		return common.Address{}, fmt.Errorf("failed to unpack factory result: %w", err)
	}

	return result, nil
}

// getPairAddress gets the pair address from a factory
func (m *MonitorService) getPairAddress(ctx context.Context, dex *DEX, factoryAddr, tokenAAddr, tokenBAddr common.Address) (common.Address, error) {
	// Use simulated value if enabled
	if os.Getenv("SIMULATION_MODE") == "1" {
		// Create a deterministic but unique address for simulation
		addrHex := fmt.Sprintf("0x%x%x", tokenAAddr[0:10], tokenBAddr[0:10])
		return common.HexToAddress(addrHex), nil
	}

	var methodName string
	var args []interface{}

	if dex.Config.Version == "v2" {
		methodName = "getPair"
		args = []interface{}{tokenAAddr, tokenBAddr}
	} else if dex.Config.Version == "v3" {
		methodName = "getPool"
		args = []interface{}{tokenAAddr, tokenBAddr, uint32(dex.Config.FeePercent)}
	} else {
		return common.Address{}, fmt.Errorf("unsupported DEX version: %s", dex.Config.Version)
	}

	data, err := dex.CallContract(ctx, factoryAddr, dex.factoryABI, methodName, args...)
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to call %s: %w", methodName, err)
	}

	var result common.Address
	if err := dex.factoryABI.UnpackIntoInterface(&result, methodName, data); err != nil {
		return common.Address{}, fmt.Errorf("failed to unpack %s result: %w", methodName, err)
	}

	return result, nil
}

// getToken0 gets token0 from a pair or pool
func (m *MonitorService) getToken0(ctx context.Context, dex *DEX, pairAddr common.Address) (common.Address, error) {
	// Use simulated value if enabled
	if os.Getenv("SIMULATION_MODE") == "1" {
		return common.HexToAddress("0x1111111111111111111111111111111111111111"), nil
	}

	data, err := dex.CallContract(ctx, pairAddr, dex.pairOrPoolABI, "token0")
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to call token0(): %w", err)
	}

	var result common.Address
	if err := dex.pairOrPoolABI.UnpackIntoInterface(&result, "token0", data); err != nil {
		return common.Address{}, fmt.Errorf("failed to unpack token0 result: %w", err)
	}

	return result, nil
}

// getReserves gets reserves from a V2 pair
func (m *MonitorService) getReserves(ctx context.Context, dex *DEX, pairAddr common.Address) (*big.Int, *big.Int, error) {
	// Use simulated values if enabled
	if os.Getenv("SIMULATION_MODE") == "1" {
		reserve0 := new(big.Int).Mul(big.NewInt(1000000), big.NewInt(1000000000000))
		reserve1 := new(big.Int).Mul(big.NewInt(2000000), big.NewInt(1000000000000))
		return reserve0, reserve1, nil
	}

	data, err := dex.CallContract(ctx, pairAddr, dex.pairOrPoolABI, "getReserves")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to call getReserves(): %w", err)
	}

	type Reserves struct {
		Reserve0           *big.Int
		Reserve1           *big.Int
		BlockTimestampLast uint32
	}

	var reserves Reserves
	if err := dex.pairOrPoolABI.UnpackIntoInterface(&reserves, "getReserves", data); err != nil {
		return nil, nil, fmt.Errorf("failed to unpack getReserves result: %w", err)
	}

	return reserves.Reserve0, reserves.Reserve1, nil
}

// CallContract is a helper method for making contract calls
func (d *DEX) CallContract(ctx context.Context, address common.Address, abi abi.ABI, method string, args ...interface{}) ([]byte, error) {
	input, err := abi.Pack(method, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to pack input for %s: %w", method, err)
	}

	msg := ethereum.CallMsg{
		To:   &address,
		Data: input,
	}

	return d.client.CallContract(ctx, msg, nil)
}

// detectArbitrageOpportunities checks for arbitrage opportunities across DEXes
func (m *MonitorService) detectArbitrageOpportunities(pairKey string, dexInfos map[string]struct {
	version  string
	price    *big.Float
	reserveA *big.Int
	reserveB *big.Int
}, tokenA, tokenB Token, lenderPreference string) {
	// Find lowest and highest price DEXes
	var lowestDEX, highestDEX string
	var lowestPrice, highestPrice *big.Float
	var lowestVersion, highestVersion string
	var lowestReserveA, lowestReserveB, highestReserveA, highestReserveB *big.Int

	for dexName, info := range dexInfos {
		if lowestPrice == nil || info.price.Cmp(lowestPrice) < 0 {
			lowestDEX = dexName
			lowestPrice = info.price
			lowestVersion = info.version
			lowestReserveA = info.reserveA
			lowestReserveB = info.reserveB
		}

		if highestPrice == nil || info.price.Cmp(highestPrice) > 0 {
			highestDEX = dexName
			highestPrice = info.price
			highestVersion = info.version
			highestReserveA = info.reserveA
			highestReserveB = info.reserveB
		}
	}

	// Calculate price difference
	if lowestDEX != highestDEX {
		// Calculate price gap
		priceGap := new(big.Float).Sub(highestPrice, lowestPrice)

		// Calculate price gap percentage
		priceGapPercent := new(big.Float).Quo(
			new(big.Float).Mul(priceGap, big.NewFloat(100)),
			lowestPrice,
		)

		// Get minimum profit threshold in USD
		minProfitUSD := big.NewFloat(1.0) // Default $1 USD
		minProfitStr := m.config.MinProfit

		if minProfitStr != "" {
			// If the config value is in token-specific units (like USDC wei), convert to USD
			minProfitValue, ok := new(big.Float).SetString(minProfitStr)
			if ok {
				// Check if this is a stablecoin amount that needs decimal adjustment
				// For USDC with 6 decimals, divide by 10^6
				if strings.Contains(strings.ToLower(pairKey), "usdc") ||
					strings.Contains(strings.ToLower(pairKey), "usdt") ||
					strings.Contains(strings.ToLower(pairKey), "dai") {
					decimals := 6 // Most stablecoins use 6 decimals
					if strings.Contains(strings.ToLower(pairKey), "dai") {
						decimals = 18 // DAI uses 18 decimals
					}

					divisor := new(big.Float).SetInt(new(big.Int).Exp(
						big.NewInt(10), big.NewInt(int64(decimals)), nil,
					))
					minProfitUSD = new(big.Float).Quo(minProfitValue, divisor)
				} else {
					// Not a stablecoin pair, use as is
					minProfitUSD = minProfitValue
				}
			}
		}

		// IMPROVED: Use a more realistic gas estimate
		gasPrice, _ := new(big.Int).SetString(m.config.GasSettings.MaxGasPrice, 10)
		gasLimit := big.NewInt(300000) // Reduced from 500,000 to a more realistic 300,000
		gasCostWei := new(big.Int).Mul(gasPrice, gasLimit)

		// Convert gas cost to ETH
		gasCostETH := new(big.Float).Quo(
			new(big.Float).SetInt(gasCostWei),
			new(big.Float).SetInt(big.NewInt(1000000000000000000)), // 10^18 (wei to ETH)
		)

		// Use current ETH price for better accuracy
		ethPriceInUSD := big.NewFloat(2500) // Updated from 2000 to 2500

		// Calculate gas cost in USD
		gasCostUSD := new(big.Float).Mul(gasCostETH, ethPriceInUSD)

		// IMPROVED: Calculate optimal trade size based on price difference and reserves
		// We'll use both sets of reserves to determine which is limiting
		reserveAFloat := new(big.Float).SetInt(lowestReserveA)
		highestReserveAFloat := new(big.Float).SetInt(highestReserveA)

		// Convert B reserves to floats as well - used for calculating max swap amounts
		reserveBFloat := new(big.Float).SetInt(lowestReserveB)
		highestReserveBFloat := new(big.Float).SetInt(highestReserveB)

		var tradeSizePercent *big.Float

		// Get price gap percentage as float for easier handling
		percentFloat, _ := priceGapPercent.Float64()

		// Dynamically adjust trade size based on price difference
		if percentFloat > 20 {
			tradeSizePercent = big.NewFloat(0.02) // 2% for huge opportunities (>20%)
		} else if percentFloat > 10 {
			tradeSizePercent = big.NewFloat(0.01) // 1% for large opportunities (10-20%)
		} else if percentFloat > 5 {
			tradeSizePercent = big.NewFloat(0.005) // 0.5% for medium opportunities (5-10%)
		} else {
			tradeSizePercent = big.NewFloat(0.002) // 0.2% for small opportunities (<5%)
		}

		// Calculate base trade size in token A using lowest DEX's reserves
		tradeAmountInTokenA := new(big.Float).Mul(reserveAFloat, tradeSizePercent)

		// Also calculate a trade size based on highest DEX's reserves
		highestTradeAmountInTokenA := new(big.Float).Mul(highestReserveAFloat, tradeSizePercent)

		// Take the smaller of the two to ensure the trade will work in both DEXes
		if highestTradeAmountInTokenA.Cmp(tradeAmountInTokenA) < 0 {
			tradeAmountInTokenA = highestTradeAmountInTokenA
		}

		// Also check reserve B limits (useful for determining maximum swap amounts)
		// This helps prevent situations where the token B reserves are too low for the trade
		tradeAmountInTokenB := new(big.Float).Mul(reserveBFloat, tradeSizePercent)
		highestTradeAmountInTokenB := new(big.Float).Mul(highestReserveBFloat, tradeSizePercent)

		// Make sure we're not trying to swap more than available in reserve B
		if highestTradeAmountInTokenB.Cmp(tradeAmountInTokenB) < 0 {
			// Adjust tokenA amount based on reserveB limitations
			tradeAmountInTokenB = highestTradeAmountInTokenB

			// Convert back to tokenA equivalent using the price ratio
			tradeAmountInTokenA = new(big.Float).Quo(tradeAmountInTokenB, lowestPrice)
		}

		// Convert to human-readable token units by dividing by 10^decimals
		divisor := new(big.Float).SetInt(new(big.Int).Exp(
			big.NewInt(10), big.NewInt(int64(tokenA.Decimals)), nil,
		))
		tradeAmountHumanReadable := new(big.Float).Quo(tradeAmountInTokenA, divisor)

		// Calculate trade value in USD with better accuracy
		var tradeValueUSD *big.Float

		if isStablecoin(tokenA.Symbol) {
			// If token A is a stablecoin, its value is approximately its amount
			tradeValueUSD = new(big.Float).Copy(tradeAmountHumanReadable)
		} else if isStablecoin(tokenB.Symbol) {
			// If token B is a stablecoin, multiply by the average price
			avgPrice := new(big.Float).Add(lowestPrice, highestPrice)
			avgPrice = new(big.Float).Quo(avgPrice, big.NewFloat(2))
			tradeValueUSD = new(big.Float).Mul(tradeAmountHumanReadable, avgPrice)
		} else {
			// IMPROVED: Better estimate for non-stablecoin pairs
			// This would ideally use a price oracle, but we'll make a more reasonable estimate
			// Use the reserves to calculate an approximate USD value
			// For example, if reserveB is a significant amount of ETH/WETH, we can estimate value

			// Check if either token might be WETH or ETH
			isTokenAEth := strings.Contains(strings.ToLower(tokenA.Symbol), "eth")
			isTokenBEth := strings.Contains(strings.ToLower(tokenB.Symbol), "eth")

			if isTokenAEth {
				// If trading ETH, just multiply by ETH price
				tradeValueUSD = new(big.Float).Mul(tradeAmountHumanReadable, ethPriceInUSD)
			} else if isTokenBEth {
				// If trading against ETH, convert to ETH value first then to USD
				ethValue := new(big.Float).Mul(tradeAmountHumanReadable, lowestPrice)
				tradeValueUSD = new(big.Float).Mul(ethValue, ethPriceInUSD)
			} else {
				// Default fallback
				tradeValueUSD = big.NewFloat(500) // Default to a moderate value for testing
			}
		}

		// IMPROVED: Calculate profit per token (just the price difference)
		profitPerToken := new(big.Float).Copy(priceGap)

		// Calculate profit in token units - this is more accurate
		profitInToken := new(big.Float).Mul(tradeAmountHumanReadable, new(big.Float).Quo(priceGapPercent, big.NewFloat(100)))

		// IMPROVED: Calculate profit in USD more accurately
		// This is the amount of tokens × price difference
		profitInUSD := new(big.Float).Mul(tradeAmountHumanReadable, priceGap)

		// Get raw profit before gas costs
		rawProfitUSD := new(big.Float).Copy(profitInUSD)

		// Subtract gas cost for net profit
		netProfitUSD := new(big.Float).Sub(profitInUSD, gasCostUSD)

		// FLASH LOAN CALCULATION
		// Calculate optimal flash loan amount for maximum profit
		flashLoanFeePercent := big.NewFloat(0.09) // 0.09% fee for Aave V2

		// Calculate maximum possible trade size based on DEX reserves
		// For optimal flash loan, we need to consider:
		// 1. DEX slippage as size increases
		// 2. Flash loan fees
		// 3. Gas costs

		// A simplified approach is to estimate using a larger percentage of reserves
		flashLoanPercent := big.NewFloat(0.05) // Try with 5% of reserves
		flashLoanAmountTokenA := new(big.Float).Mul(reserveAFloat, flashLoanPercent)

		// Make sure we're not exceeding reserve B limitations
		flashLoanAmountTokenB := new(big.Float).Mul(reserveBFloat, flashLoanPercent)
		highestFlashLoanAmountTokenB := new(big.Float).Mul(highestReserveBFloat, flashLoanPercent)

		// Use the more limiting factor
		if highestFlashLoanAmountTokenB.Cmp(flashLoanAmountTokenB) < 0 {
			flashLoanAmountTokenB = highestFlashLoanAmountTokenB
			flashLoanAmountTokenA = new(big.Float).Quo(flashLoanAmountTokenB, lowestPrice)
		}

		// Convert to human-readable amount
		flashLoanHumanReadable := new(big.Float).Quo(flashLoanAmountTokenA, divisor)

		// Calculate flash loan fee
		flashLoanFee := new(big.Float).Mul(
			flashLoanHumanReadable,
			new(big.Float).Quo(flashLoanFeePercent, big.NewFloat(100)),
		)

		// Calculate potential profit from flash loan
		flashLoanProfitPerToken := new(big.Float).Copy(profitPerToken)
		flashLoanRawProfit := new(big.Float).Mul(flashLoanHumanReadable, flashLoanProfitPerToken)

		// Subtract flash loan fee
		flashLoanProfitAfterFee := new(big.Float).Sub(flashLoanRawProfit, flashLoanFee)

		// Subtract gas costs
		flashLoanNetProfit := new(big.Float).Sub(flashLoanProfitAfterFee, gasCostUSD)

		// Calculate flash loan profit in USD
		flashLoanProfitUSD := flashLoanNetProfit

		// Convert to USD if needed
		if !isStablecoin(tokenA.Symbol) && isStablecoin(tokenB.Symbol) {
			// If the asset isn't a stablecoin but we're trading against one,
			// multiply by average price to get USD value
			avgPrice := new(big.Float).Add(lowestPrice, highestPrice)
			avgPrice = new(big.Float).Quo(avgPrice, big.NewFloat(2))
			flashLoanProfitUSD = new(big.Float).Mul(flashLoanNetProfit, avgPrice)
		}

		// Calculate flash loan trade value in USD
		var flashLoanValueUSD *big.Float

		if isStablecoin(tokenA.Symbol) {
			flashLoanValueUSD = new(big.Float).Copy(flashLoanHumanReadable)
		} else if isStablecoin(tokenB.Symbol) {
			avgPrice := new(big.Float).Add(lowestPrice, highestPrice)
			avgPrice = new(big.Float).Quo(avgPrice, big.NewFloat(2))
			flashLoanValueUSD = new(big.Float).Mul(flashLoanHumanReadable, avgPrice)
		} else {
			// Use same logic as regular trade, but with flash loan amount
			isTokenAEth := strings.Contains(strings.ToLower(tokenA.Symbol), "eth")
			isTokenBEth := strings.Contains(strings.ToLower(tokenB.Symbol), "eth")

			if isTokenAEth {
				flashLoanValueUSD = new(big.Float).Mul(flashLoanHumanReadable, ethPriceInUSD)
			} else if isTokenBEth {
				ethValue := new(big.Float).Mul(flashLoanHumanReadable, lowestPrice)
				flashLoanValueUSD = new(big.Float).Mul(ethValue, ethPriceInUSD)
			} else {
				// Default estimate
				flashLoanValueUSD = big.NewFloat(2000) // Higher value for flash loan
			}
		}

		// Get dynamic profit threshold based on gas costs
		dynamicMinProfit := new(big.Float).Mul(gasCostUSD, big.NewFloat(1.2))    // 20% above gas
		dynamicMinProfit = new(big.Float).Add(dynamicMinProfit, big.NewFloat(1)) // plus $1 base profit

		// Don't go below configured minimum
		if dynamicMinProfit.Cmp(minProfitUSD) < 0 {
			dynamicMinProfit = minProfitUSD
		}

		// Check both normal trade and flash loan viability
		isViable := netProfitUSD.Cmp(dynamicMinProfit) > 0 && percentFloat >= 0.5
		isFlashLoanViable := flashLoanNetProfit.Cmp(dynamicMinProfit) > 0 && percentFloat >= 0.5

		// High percentage opportunities with positive profit are viable even with lower absolute profit
		if percentFloat >= 5.0 && netProfitUSD.Cmp(big.NewFloat(0)) > 0 {
			isViable = true
		}

		if percentFloat >= 3.0 && flashLoanNetProfit.Cmp(big.NewFloat(0)) > 0 {
			isFlashLoanViable = true
		}

		// Create opportunity with proper scaling
		opportunity := &ArbitrageOpportunity{
			Timestamp:         time.Now(),
			PairKey:           pairKey,
			BuyDEX:            lowestDEX,
			SellDEX:           highestDEX,
			BuyDEXVersion:     lowestVersion,
			SellDEXVersion:    highestVersion,
			BuyPrice:          lowestPrice,
			SellPrice:         highestPrice,
			PriceGap:          priceGap,
			ProfitPercent:     priceGapPercent,
			GasCost:           gasCostUSD,
			EstimatedProfit:   netProfitUSD,
			ProfitInUSD:       rawProfitUSD,
			ProfitInToken:     profitInToken,
			TradeAmountUSD:    tradeValueUSD,
			TradeAmountToken:  tradeAmountHumanReadable,
			IsViable:          isViable,
			LenderUsed:        getLenderForPair(m.config, lenderPreference),
			ReserveA:          lowestReserveA,
			ReserveB:          lowestReserveB,
			RecommendedAmount: tradeAmountHumanReadable,

			// Add flash loan information to the opportunity
			FlashLoanAmount:    flashLoanHumanReadable,
			FlashLoanProfitUSD: flashLoanProfitUSD,
			FlashLoanValueUSD:  flashLoanValueUSD,
			IsFlashLoanViable:  isFlashLoanViable,
		}

		// Log opportunity with the improved logger
		m.logOpportunity(opportunity)

		// Store opportunity
		m.opportunityMu.Lock()
		m.opportunities = append(m.opportunities, opportunity)
		m.opportunityMu.Unlock()

		// NEW CODE: Send viable opportunities to the executor
		if opportunity.IsViable || opportunity.IsFlashLoanViable {
			// Create context with timeout for execution
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			// Execute the opportunity (this will simulate or execute based on configuration)
			go m.executor.ExecuteArbitrageOpportunity(ctx, opportunity)
		}
	}
}

func (m *MonitorService) logOpportunity(opp *ArbitrageOpportunity) {
	// Use the existing logger's mutex
	m.logger.mu.Lock()
	defer m.logger.mu.Unlock()

	// Format profits with appropriate scaling
	rawProfitFloat, _ := opp.ProfitInUSD.Float64()
	netProfitFloat, _ := opp.EstimatedProfit.Float64()
	gasCostFloat, _ := opp.GasCost.Float64()

	rawProfitStr := fmt.Sprintf("$%.2f", rawProfitFloat)
	netProfitStr := fmt.Sprintf("$%.2f", netProfitFloat)

	buyPriceStr := opp.BuyPrice.Text('f', 6)
	sellPriceStr := opp.SellPrice.Text('f', 6)

	// Get percentage as float
	percentFloat, _ := opp.ProfitPercent.Float64()

	// Get trade amount
	tradeAmountFloat, _ := opp.TradeAmountToken.Float64()
	tradeValueFloat, _ := opp.TradeAmountUSD.Float64()

	// Flash loan info
	flashLoanAmountFloat, _ := opp.FlashLoanAmount.Float64()
	flashLoanProfitFloat, _ := opp.FlashLoanProfitUSD.Float64()
	flashLoanValueFloat, _ := opp.FlashLoanValueUSD.Float64()

	// Show most profitable opportunity first
	if opp.IsFlashLoanViable && flashLoanProfitFloat > netProfitFloat {
		color.New(color.FgHiWhite, color.BgGreen).Printf(
			"[FLASH LOAN OPPORTUNITY] %s: Buy on %s (%s) at %s, sell on %s (%s) at %s. Profit: $%.2f (%.2f%%) 💸\n",
			opp.PairKey, opp.BuyDEX, opp.BuyDEXVersion, buyPriceStr,
			opp.SellDEX, opp.SellDEXVersion, sellPriceStr, flashLoanProfitFloat, percentFloat,
		)

		fmt.Printf("  Flash Loan: %.4f tokens ($%.2f) | Gas: $%.2f | Net Profit: $%.2f\n",
			flashLoanAmountFloat, flashLoanValueFloat, gasCostFloat, flashLoanProfitFloat)

		// If normal trade is also viable, show that too
		if opp.IsViable {
			fmt.Printf("  Regular Trade: %.4f tokens ($%.2f) | Net Profit: %s\n",
				tradeAmountFloat, tradeValueFloat, netProfitStr)
		}
	} else if opp.IsViable {
		color.New(color.FgHiWhite, color.BgGreen).Printf(
			"[VIABLE OPPORTUNITY] %s: Buy on %s (%s) at %s, sell on %s (%s) at %s. Profit: %s (%.2f%%) 🚀\n",
			opp.PairKey, opp.BuyDEX, opp.BuyDEXVersion, buyPriceStr,
			opp.SellDEX, opp.SellDEXVersion, sellPriceStr, netProfitStr, percentFloat,
		)

		// Add detailed information about the trade
		fmt.Printf("  Trade: %.4f tokens ($%.2f) | Gas: $%.2f | Raw Profit: %s | Net Profit: %s\n",
			tradeAmountFloat, tradeValueFloat, gasCostFloat, rawProfitStr, netProfitStr)

		// Show flash loan info if it's viable but less profitable
		if opp.IsFlashLoanViable {
			fmt.Printf("  Alternative Flash Loan: %.4f tokens | Profit: $%.2f\n",
				flashLoanAmountFloat, flashLoanProfitFloat)
		}
	} else if opp.IsFlashLoanViable {
		color.New(color.FgHiWhite, color.BgYellow).Printf(
			"[FLASH LOAN ONLY] %s: Buy on %s (%s) at %s, sell on %s (%s) at %s. Flash Loan Profit: $%.2f (%.2f%%)\n",
			opp.PairKey, opp.BuyDEX, opp.BuyDEXVersion, buyPriceStr,
			opp.SellDEX, opp.SellDEXVersion, sellPriceStr, flashLoanProfitFloat, percentFloat,
		)

		fmt.Printf("  Flash Loan: %.4f tokens ($%.2f) | Gas: $%.2f | Net Profit: $%.2f\n",
			flashLoanAmountFloat, flashLoanValueFloat, gasCostFloat, flashLoanProfitFloat)

		fmt.Printf("  Regular trade not viable: Net profit $%.2f is below threshold\n",
			netProfitFloat)
	} else if percentFloat >= 5.0 {
		// Show potential opportunities that have good percentage but aren't viable due to gas
		color.New(color.FgHiWhite, color.BgYellow).Printf(
			"[POTENTIAL OPPORTUNITY] %s: Buy on %s (%s) at %s, sell on %s (%s) at %s. Profit before gas: %s (%.2f%%)\n",
			opp.PairKey, opp.BuyDEX, opp.BuyDEXVersion, buyPriceStr,
			opp.SellDEX, opp.SellDEXVersion, sellPriceStr, rawProfitStr, percentFloat,
		)

		// Explain why it's not viable
		if netProfitFloat <= 0 {
			fmt.Printf("  Not viable: Gas cost ($%.2f) exceeds raw profit (%s)\n",
				gasCostFloat, rawProfitStr)
		} else {
			fmt.Printf("  Trade: %.4f tokens | Gas: $%.2f | Net profit too low: %s\n",
				tradeAmountFloat, gasCostFloat, netProfitStr)
		}
	} else {
		color.New(color.FgHiWhite, color.BgBlue).Printf(
			"[LOW OPPORTUNITY] %s: Buy on %s (%s) at %s, sell on %s (%s) at %s. Price diff: %.2f%% - Below threshold\n",
			opp.PairKey, opp.BuyDEX, opp.BuyDEXVersion, buyPriceStr,
			opp.SellDEX, opp.SellDEXVersion, sellPriceStr, percentFloat,
		)
	}
}

// startAPIServer starts the HTTP API server
func (m *MonitorService) startAPIServer() {
	router := mux.NewRouter()

	// Register API endpoints
	router.HandleFunc("/api/health", m.handleHealth).Methods("GET")
	router.HandleFunc("/api/pairs", m.handleGetPairs).Methods("GET")
	router.HandleFunc("/api/pairs", m.handleAddPair).Methods("POST")
	router.HandleFunc("/api/pairs/{pairKey}", m.handleGetPair).Methods("GET")
	router.HandleFunc("/api/pairs/{pairKey}", m.handleUpdatePair).Methods("PUT")
	router.HandleFunc("/api/pairs/{pairKey}", m.handleDeletePair).Methods("DELETE")
	router.HandleFunc("/api/pairs/{pairKey}/start", m.handleStartPair).Methods("POST")
	router.HandleFunc("/api/pairs/{pairKey}/stop", m.handleStopPair).Methods("POST")

	router.HandleFunc("/api/dexes", m.handleGetDEXes).Methods("GET")
	router.HandleFunc("/api/dexes", m.handleAddDEX).Methods("POST")
	router.HandleFunc("/api/dexes/{name}", m.handleGetDEX).Methods("GET")
	router.HandleFunc("/api/dexes/{name}", m.handleUpdateDEX).Methods("PUT")
	router.HandleFunc("/api/dexes/{name}", m.handleDeleteDEX).Methods("DELETE")

	router.HandleFunc("/api/tokens", m.handleGetTokens).Methods("GET")
	router.HandleFunc("/api/tokens", m.handleAddToken).Methods("POST")

	router.HandleFunc("/api/opportunities", m.handleGetOpportunities).Methods("GET")
	router.HandleFunc("/api/bots", m.handleGetBots).Methods("GET")

	router.HandleFunc("/api/execution/results", m.handleGetExecutionResults).Methods("GET")
	router.HandleFunc("/api/execution/mode", m.handleSetSimulationMode).Methods("POST")
	// Start server
	port := m.config.APIPort
	if port == "" {
		port = "8080"
	}

	m.logger.Info("Starting API server on port %s", port)
	if err := http.ListenAndServe(":"+port, router); err != nil {
		m.logger.Error("API server error: %v", err)
	}
}

// API Handlers

func (m *MonitorService) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (m *MonitorService) handleGetPairs(w http.ResponseWriter, r *http.Request) {
	m.pairMutex.RLock()
	defer m.pairMutex.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(m.tokenPairs)
}

func (m *MonitorService) handleAddPair(w http.ResponseWriter, r *http.Request) {
	var pair TokenPair
	if err := json.NewDecoder(r.Body).Decode(&pair); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Validate pair
	if pair.TokenA == "" || pair.TokenB == "" {
		http.Error(w, "TokenA and TokenB are required", http.StatusBadRequest)
		return
	}

	// Check if tokens exist
	if _, exists := m.config.TokenAddresses[pair.TokenA]; !exists {
		http.Error(w, fmt.Sprintf("Token %s not found", pair.TokenA), http.StatusBadRequest)
		return
	}

	if _, exists := m.config.TokenAddresses[pair.TokenB]; !exists {
		http.Error(w, fmt.Sprintf("Token %s not found", pair.TokenB), http.StatusBadRequest)
		return
	}

	// Set pair key if not provided
	if pair.PairKey == "" {
		pair.PairKey = fmt.Sprintf("%s-%s", pair.TokenA, pair.TokenB)
	}

	// Check if pair already exists
	m.pairMutex.Lock()
	if _, exists := m.tokenPairs[pair.PairKey]; exists {
		m.pairMutex.Unlock()
		http.Error(w, fmt.Sprintf("Pair %s already exists", pair.PairKey), http.StatusConflict)
		return
	}

	// Set active by default
	pair.Active = true

	// If no DEXes specified, use all available DEXes
	if len(pair.DEXes) == 0 {
		for dexName := range m.dexes {
			pair.DEXes = append(pair.DEXes, dexName)
		}
	}

	// Add pair
	m.tokenPairs[pair.PairKey] = &pair
	m.pairMutex.Unlock()

	// Create bot instance
	m.botsMutex.Lock()
	m.bots[pair.PairKey] = &BotInstance{
		TokenPair: &pair,
		Prices:    make(map[string]*big.Float),
		Status:    "stopped",
	}
	m.botsMutex.Unlock()

	// Save config
	m.saveConfig()

	// Start monitoring if active
	if pair.Active {
		m.startMonitoringPair(pair.PairKey, &pair)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(pair)
}

func (m *MonitorService) handleGetPair(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	pairKey := vars["pairKey"]

	m.pairMutex.RLock()
	pair, exists := m.tokenPairs[pairKey]
	m.pairMutex.RUnlock()

	if !exists {
		http.Error(w, fmt.Sprintf("Pair %s not found", pairKey), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(pair)
}

func (m *MonitorService) handleUpdatePair(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	pairKey := vars["pairKey"]

	// Check if pair exists
	m.pairMutex.RLock()
	_, exists := m.tokenPairs[pairKey]
	m.pairMutex.RUnlock()

	if !exists {
		http.Error(w, fmt.Sprintf("Pair %s not found", pairKey), http.StatusNotFound)
		return
	}

	// Parse request body
	var updatedPair TokenPair
	if err := json.NewDecoder(r.Body).Decode(&updatedPair); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Update pair (don't change the key)
	updatedPair.PairKey = pairKey

	m.pairMutex.Lock()
	m.tokenPairs[pairKey] = &updatedPair
	m.pairMutex.Unlock()

	// Update bot status
	m.botsMutex.Lock()
	if bot, exists := m.bots[pairKey]; exists {
		bot.TokenPair = &updatedPair
	}
	m.botsMutex.Unlock()

	// Save config
	m.saveConfig()

	// Handle active state change
	wasActive := m.isPairBeingMonitored(pairKey)
	shouldBeActive := updatedPair.Active

	if !wasActive && shouldBeActive {
		m.startMonitoringPair(pairKey, &updatedPair)
	} else if wasActive && !shouldBeActive {
		m.stopMonitoringPair(pairKey)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(updatedPair)
}

func (m *MonitorService) handleDeletePair(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	pairKey := vars["pairKey"]

	// Check if pair exists
	m.pairMutex.RLock()
	_, exists := m.tokenPairs[pairKey]
	m.pairMutex.RUnlock()

	if !exists {
		http.Error(w, fmt.Sprintf("Pair %s not found", pairKey), http.StatusNotFound)
		return
	}

	// Stop monitoring if active
	if m.isPairBeingMonitored(pairKey) {
		m.stopMonitoringPair(pairKey)
	}

	// Remove pair
	m.pairMutex.Lock()
	delete(m.tokenPairs, pairKey)
	m.pairMutex.Unlock()

	// Remove bot
	m.botsMutex.Lock()
	delete(m.bots, pairKey)
	m.botsMutex.Unlock()

	// Clean up price cache
	m.priceMutex.Lock()
	delete(m.pairPrices, pairKey)
	m.priceMutex.Unlock()

	// Save config
	m.saveConfig()

	w.WriteHeader(http.StatusNoContent)
}

func (m *MonitorService) handleStartPair(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	pairKey := vars["pairKey"]

	// Check if pair exists
	m.pairMutex.RLock()
	pair, exists := m.tokenPairs[pairKey]
	m.pairMutex.RUnlock()

	if !exists {
		http.Error(w, fmt.Sprintf("Pair %s not found", pairKey), http.StatusNotFound)
		return
	}

	// Check if already monitoring
	if m.isPairBeingMonitored(pairKey) {
		http.Error(w, fmt.Sprintf("Pair %s is already being monitored", pairKey), http.StatusConflict)
		return
	}

	// Update active status
	m.pairMutex.Lock()
	pair.Active = true
	m.pairMutex.Unlock()

	// Start monitoring
	m.startMonitoringPair(pairKey, pair)

	// Save config
	m.saveConfig()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "started"})
}

func (m *MonitorService) handleStopPair(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	pairKey := vars["pairKey"]

	// Check if pair exists
	m.pairMutex.RLock()
	pair, exists := m.tokenPairs[pairKey]
	m.pairMutex.RUnlock()

	if !exists {
		http.Error(w, fmt.Sprintf("Pair %s not found", pairKey), http.StatusNotFound)
		return
	}

	// Check if monitoring
	if !m.isPairBeingMonitored(pairKey) {
		http.Error(w, fmt.Sprintf("Pair %s is not being monitored", pairKey), http.StatusConflict)
		return
	}

	// Update active status
	m.pairMutex.Lock()
	pair.Active = false
	m.pairMutex.Unlock()

	// Stop monitoring
	m.stopMonitoringPair(pairKey)

	// Save config
	m.saveConfig()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "stopped"})
}

func (m *MonitorService) handleGetDEXes(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(m.config.DEXes)
}

func (m *MonitorService) handleAddDEX(w http.ResponseWriter, r *http.Request) {
	var dexConfig DEXConfig
	if err := json.NewDecoder(r.Body).Decode(&dexConfig); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Validate DEX config
	if dexConfig.Name == "" || dexConfig.RouterAddress == (common.Address{}) {
		http.Error(w, "Name and RouterAddress are required", http.StatusBadRequest)
		return
	}

	// Check version
	if dexConfig.Version != "v2" && dexConfig.Version != "v3" {
		http.Error(w, "Version must be 'v2' or 'v3'", http.StatusBadRequest)
		return
	}

	// V3 requires a fee percent
	if dexConfig.Version == "v3" && dexConfig.FeePercent == 0 {
		dexConfig.FeePercent = 3000 // Default to 0.3%
	}

	// Check if DEX already exists
	if _, exists := m.config.DEXes[dexConfig.Name]; exists {
		http.Error(w, fmt.Sprintf("DEX %s already exists", dexConfig.Name), http.StatusConflict)
		return
	}

	// Add to config
	if m.config.DEXes == nil {
		m.config.DEXes = make(map[string]DEXConfig)
	}
	m.config.DEXes[dexConfig.Name] = dexConfig

	// Create DEX instance
	newDEXes, err := createDEXInstances(map[string]DEXConfig{dexConfig.Name: dexConfig}, m.client, m.logger)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create DEX instance: %v", err), http.StatusInternalServerError)
		return
	}

	// Add to dexes map
	for name, dex := range newDEXes {
		m.dexes[name] = dex
	}

	// Save config
	m.saveConfig()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(dexConfig)
}

func (m *MonitorService) handleGetDEX(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	dexName := vars["name"]

	dexConfig, exists := m.config.DEXes[dexName]
	if !exists {
		http.Error(w, fmt.Sprintf("DEX %s not found", dexName), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(dexConfig)
}

func (m *MonitorService) handleUpdateDEX(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	dexName := vars["name"]

	// Check if DEX exists
	if _, exists := m.config.DEXes[dexName]; !exists {
		http.Error(w, fmt.Sprintf("DEX %s not found", dexName), http.StatusNotFound)
		return
	}

	// Parse request body
	var updatedDEX DEXConfig
	if err := json.NewDecoder(r.Body).Decode(&updatedDEX); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Ensure name matches
	updatedDEX.Name = dexName

	// Update DEX config
	m.config.DEXes[dexName] = updatedDEX

	// Recreate DEX instance
	newDEXes, err := createDEXInstances(map[string]DEXConfig{dexName: updatedDEX}, m.client, m.logger)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create DEX instance: %v", err), http.StatusInternalServerError)
		return
	}

	// Update DEX instance
	for name, dex := range newDEXes {
		m.dexes[name] = dex
	}

	// Save config
	m.saveConfig()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(updatedDEX)
}

func (m *MonitorService) handleDeleteDEX(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	dexName := vars["name"]

	// Check if DEX exists
	if _, exists := m.config.DEXes[dexName]; !exists {
		http.Error(w, fmt.Sprintf("DEX %s not found", dexName), http.StatusNotFound)
		return
	}

	// Check if any pairs are using this DEX
	isInUse := false
	m.pairMutex.RLock()
	for _, pair := range m.tokenPairs {
		for _, usedDEX := range pair.DEXes {
			if usedDEX == dexName {
				isInUse = true
				break
			}
		}
		if isInUse {
			break
		}
	}
	m.pairMutex.RUnlock()

	if isInUse {
		http.Error(w, fmt.Sprintf("DEX %s is in use by one or more pairs", dexName), http.StatusConflict)
		return
	}

	// Delete DEX config
	delete(m.config.DEXes, dexName)

	// Delete DEX instance
	delete(m.dexes, dexName)

	// Save config
	m.saveConfig()

	w.WriteHeader(http.StatusNoContent)
}

func (m *MonitorService) handleGetTokens(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(m.config.TokenAddresses)
}

func (m *MonitorService) handleAddToken(w http.ResponseWriter, r *http.Request) {
	var token Token
	if err := json.NewDecoder(r.Body).Decode(&token); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Validate token
	if token.Symbol == "" || token.Address == (common.Address{}) {
		http.Error(w, "Symbol and Address are required", http.StatusBadRequest)
		return
	}

	// Check if token already exists
	if _, exists := m.config.TokenAddresses[token.Symbol]; exists {
		http.Error(w, fmt.Sprintf("Token %s already exists", token.Symbol), http.StatusConflict)
		return
	}

	// Add to config
	if m.config.TokenAddresses == nil {
		m.config.TokenAddresses = make(map[string]Token)
	}
	m.config.TokenAddresses[token.Symbol] = token

	// Try to get token decimals if not provided
	if token.Decimals == 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Try to get decimals from the first DEX
		for _, dex := range m.dexes {
			decimals, err := m.getTokenDecimals(ctx, dex, token.Address)
			if err == nil {
				token.Decimals = decimals
				// Update in config
				m.config.TokenAddresses[token.Symbol] = token
				break
			}
		}
	}

	// Save config
	m.saveConfig()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(token)
}

func (m *MonitorService) handleGetOpportunities(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters
	query := r.URL.Query()
	limitStr := query.Get("limit")
	viableOnly := query.Get("viable") == "true"
	pairKey := query.Get("pair")

	// Parse limit
	limit := 100
	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	// Filter opportunities
	m.opportunityMu.RLock()
	defer m.opportunityMu.RUnlock()

	var filteredOpportunities []*ArbitrageOpportunity
	for _, opp := range m.opportunities {
		// Apply filters
		if viableOnly && !opp.IsViable {
			continue
		}

		if pairKey != "" && opp.PairKey != pairKey {
			continue
		}

		filteredOpportunities = append(filteredOpportunities, opp)

		// Apply limit
		if len(filteredOpportunities) >= limit {
			break
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(filteredOpportunities)
}

func (m *MonitorService) handleGetBots(w http.ResponseWriter, r *http.Request) {
	m.botsMutex.RLock()
	defer m.botsMutex.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(m.bots)
}

// Add an API endpoint to get execution results
func (m *MonitorService) handleGetExecutionResults(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	results := m.executor.GetExecutionResults()
	json.NewEncoder(w).Encode(results)
}

// Add an API endpoint to toggle simulation mode
func (m *MonitorService) handleSetSimulationMode(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Simulation bool `json:"simulation"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	m.executor.SetSimulationMode(request.Simulation)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "success",
		"mode":   map[bool]string{true: "simulation", false: "real_execution"}[request.Simulation],
	})
}

// Helper Methods

func (m *MonitorService) isPairBeingMonitored(pairKey string) bool {
	m.workerMutex.Lock()
	defer m.workerMutex.Unlock()
	_, exists := m.pairWorkers[pairKey]
	return exists
}

func (m *MonitorService) getTokenDecimals(ctx context.Context, dex *DEX, tokenAddr common.Address) (uint8, error) {
	// Use simulated value if enabled
	if os.Getenv("SIMULATION_MODE") == "1" {
		// Return common decimals based on known tokens
		addrStr := tokenAddr.Hex()
		if strings.Contains(strings.ToLower(addrStr), "usdc") ||
			strings.Contains(strings.ToLower(addrStr), "usdt") ||
			strings.Contains(strings.ToLower(addrStr), "dai") {
			return 6, nil
		}
		if strings.Contains(strings.ToLower(addrStr), "wbtc") {
			return 8, nil
		}
		return 18, nil // Default for most tokens
	}

	data, err := dex.CallContract(ctx, tokenAddr, dex.erc20ABI, "decimals")
	if err != nil {
		return 0, fmt.Errorf("failed to call decimals(): %w", err)
	}

	var result uint8
	if err := dex.erc20ABI.UnpackIntoInterface(&result, "decimals", data); err != nil {
		return 0, fmt.Errorf("failed to unpack decimals result: %w", err)
	}

	return result, nil
}

func (m *MonitorService) saveOpportunities() {
	m.opportunityMu.RLock()
	defer m.opportunityMu.RUnlock()

	if len(m.opportunities) == 0 {
		return
	}

	// Create opportunities directory if it doesn't exist
	if err := os.MkdirAll("opportunities", 0755); err != nil {
		m.logger.Error("Failed to create opportunities directory: %v", err)
		return
	}

	// Save opportunities to file with timestamp
	filename := fmt.Sprintf("opportunities/opportunities_%s.json", time.Now().Format("20060102_150405"))
	data, err := json.MarshalIndent(m.opportunities, "", "  ")
	if err != nil {
		m.logger.Error("Failed to marshal opportunities: %v", err)
		return
	}

	if err := ioutil.WriteFile(filename, data, 0644); err != nil {
		m.logger.Error("Failed to write opportunities file: %v", err)
		return
	}

	m.logger.Info("Saved %d opportunities to %s", len(m.opportunities), filename)
}

func (m *MonitorService) saveConfig() {
	// Update token pairs in config
	m.pairMutex.RLock()
	var tokenPairs []TokenPair
	for _, pair := range m.tokenPairs {
		tokenPairs = append(tokenPairs, *pair)
	}
	m.pairMutex.RUnlock()

	m.config.TokenPairs = tokenPairs

	// Marshal config
	data, err := json.MarshalIndent(m.config, "", "  ")
	if err != nil {
		m.logger.Error("Failed to marshal config: %v", err)
		return
	}

	// Save to file
	if err := ioutil.WriteFile(m.configPath, data, 0644); err != nil {
		m.logger.Error("Failed to write config file: %v", err)
		return
	}

	m.logger.Info("Saved configuration to %s", m.configPath)
}

func loadConfig(path string) (*Configuration, error) {
	// Check if file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// Create default configuration
		return createDefaultConfig(path)
	}

	// Read file
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Parse JSON
	var config Configuration
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &config, nil
}

func createDefaultConfig(path string) (*Configuration, error) {
	// Create default configuration
	config := &Configuration{
		RPCEndpoints: []string{"http://localhost:8545"},
		WSEndpoints:  []string{"ws://localhost:8545"},
		PrivateKeys:  []string{},
		TokenAddresses: map[string]Token{
			"WETH": {
				Symbol:   "WETH",
				Address:  common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
				Decimals: 18,
			},
			"USDC": {
				Symbol:   "USDC",
				Address:  common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
				Decimals: 6,
			},
		},
		DEXes: map[string]DEXConfig{
			"UniswapV2": {
				Name:          "UniswapV2",
				Version:       "v2",
				RouterAddress: common.HexToAddress("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"),
				FeePercent:    3000,
			},
			"SushiswapV2": {
				Name:          "SushiswapV2",
				Version:       "v2",
				RouterAddress: common.HexToAddress("0xd9e1cE17f2641f24aE83637ab66a2cca9C378B9F"),
				FeePercent:    3000,
			},
		},
		MinProfit: "0.5",
		GasSettings: GasSettings{
			MaxGasPrice:        "50000000000", // 50 Gwei
			PriorityFee:        "1000000000",  // 1 Gwei
			GasLimitMultiplier: 1.2,
			SpeedUpThreshold:   3,
		},
		APIPort:       "8080",
		DefaultLender: "aave",
		LenderConfig: LenderConfig{
			AavePoolAddress:      common.HexToAddress("0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9"),
			BalancerVaultAddress: common.HexToAddress("0xBA12222222228d8Ba445958a75a0704d566BF2C8"),
		},
	}

	// Save config to file
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal default config: %w", err)
	}

	if err := ioutil.WriteFile(path, data, 0644); err != nil {
		return nil, fmt.Errorf("failed to write default config file: %w", err)
	}

	return config, nil
}

// Helper function to determine if a token is a stablecoin
func isStablecoin(symbol string) bool {
	symbol = strings.ToUpper(symbol)
	stablecoins := map[string]bool{
		"USDC": true,
		"USDT": true,
		"DAI":  true,
		"BUSD": true,
		"TUSD": true,
		"USDP": true,
		"GUSD": true,
		"FRAX": true,
	}
	return stablecoins[symbol]
}

// Helper function to get the lender for a pair
func getLenderForPair(config *Configuration, lenderPreference string) string {
	if lenderPreference != "" {
		return lenderPreference
	}
	return config.DefaultLender
}

// ExecuteArbitrageOpportunity processes a profitable opportunity
// and either simulates or executes the trade based on configuration
func (e *TradeExecutor) ExecuteArbitrageOpportunity(ctx context.Context, opportunity *ArbitrageOpportunity) {
	// Generate a unique key for this execution
	executionKey := fmt.Sprintf("%s-%d", opportunity.PairKey, opportunity.Timestamp.UnixNano())

	// Check if we're already processing this opportunity
	if _, exists := e.activeExecutions.Load(executionKey); exists {
		e.logger.Warning("Already processing opportunity: %s", executionKey)
		return
	}

	// Mark this opportunity as being processed
	e.activeExecutions.Store(executionKey, true)

	// Start execution timer
	startTime := time.Now()

	// Create a result to track this execution
	result := &ExecutionResult{
		Opportunity:    opportunity,
		Timestamp:      time.Now(),
		SimulationOnly: e.simulationMode,
	}

	// Choose execution method based on profitability
	var err error
	if opportunity.IsFlashLoanViable &&
		opportunity.FlashLoanProfitUSD != nil &&
		opportunity.EstimatedProfit != nil &&
		opportunity.FlashLoanProfitUSD.Cmp(opportunity.EstimatedProfit) > 0 {
		// Flash loan is more profitable
		e.logger.Info("Executing flash loan arbitrage for %s", opportunity.PairKey)
		err = e.executeFlashLoanArbitrage(ctx, opportunity, result)
	} else if opportunity.IsViable {
		// Regular arbitrage is viable
		e.logger.Info("Executing regular arbitrage for %s", opportunity.PairKey)
		err = e.executeRegularArbitrage(ctx, opportunity, result)
	} else {
		// This shouldn't happen, but just in case
		err = fmt.Errorf("opportunity is not viable for execution")
	}

	// Set execution time
	result.ExecutionTime = time.Since(startTime)

	// Handle any execution errors
	if err != nil {
		result.Success = false
		result.Error = err.Error()
		e.logger.Error("Arbitrage execution failed: %v", err)
	} else {
		result.Success = true
		e.logger.Success("Arbitrage execution completed successfully in %v", result.ExecutionTime)
	}

	// Store the execution result
	e.resultsMutex.Lock()
	e.executionResults = append(e.executionResults, result)
	e.resultsMutex.Unlock()

	// Remove from active executions
	e.activeExecutions.Delete(executionKey)

	// Log the execution result
	e.logExecutionResult(result)
}

// Simulation for regular arbitrage
func (e *TradeExecutor) executeRegularArbitrage(ctx context.Context, opp *ArbitrageOpportunity, result *ExecutionResult) error {
	if !e.simulationMode {
		// In real mode, this would contain the actual transaction code
		return fmt.Errorf("real execution mode not implemented")
	}

	// This is just simulation logic
	e.logger.Info("Simulating regular arbitrage execution for %s", opp.PairKey)

	// Simulate the trading steps:
	// 1. First simulate approval for token spending
	e.logger.Info("Simulating token approval...")
	time.Sleep(200 * time.Millisecond)

	// 2. Simulate swap on first DEX
	e.logger.Info("Simulating swap on %s...", opp.BuyDEX)
	time.Sleep(500 * time.Millisecond)

	// 3. Simulate swap on second DEX
	e.logger.Info("Simulating swap on %s...", opp.SellDEX)
	time.Sleep(500 * time.Millisecond)

	// Generate a fake transaction hash
	result.TxHash = common.HexToHash(fmt.Sprintf("0x%064x", time.Now().UnixNano()))

	// Simulate gas used - slightly random to mimic real behavior
	baseGas := uint64(180000)                                // Base gas for a regular arbitrage
	randomVariation := uint64(time.Now().UnixNano() % 40000) // Random variation
	result.GasUsed = baseGas + randomVariation

	// Calculate actual profit (in simulation, we'll use a random factor around estimated)
	profitFactor := 0.8 + (float64(time.Now().UnixNano()%4000) / 10000.0) // 0.8 to 1.2
	estProfit, _ := opp.EstimatedProfit.Float64()
	actualProfit := estProfit * profitFactor
	result.ActualProfit = big.NewFloat(actualProfit)

	return nil
}

// Simulation for flash loan arbitrage
func (e *TradeExecutor) executeFlashLoanArbitrage(ctx context.Context, opp *ArbitrageOpportunity, result *ExecutionResult) error {
	if !e.simulationMode {
		// In real mode, this would contain the actual transaction code
		return fmt.Errorf("real execution mode not implemented")
	}

	// This is just simulation logic
	e.logger.Info("Simulating flash loan arbitrage execution for %s", opp.PairKey)

	// Simulate the trading steps:
	e.logger.Info("Simulating flash loan from %s...", opp.LenderUsed)
	time.Sleep(300 * time.Millisecond)

	// Simulate first swap
	e.logger.Info("Simulating swap on %s...", opp.BuyDEX)
	time.Sleep(500 * time.Millisecond)

	// Simulate second swap
	e.logger.Info("Simulating swap on %s...", opp.SellDEX)
	time.Sleep(500 * time.Millisecond)

	// Simulate loan repayment
	e.logger.Info("Simulating flash loan repayment...")
	time.Sleep(200 * time.Millisecond)

	// Generate a fake transaction hash
	result.TxHash = common.HexToHash(fmt.Sprintf("0x%064x", time.Now().UnixNano()))

	// Simulate gas used - flash loans use more gas
	baseGas := uint64(350000)                                // Base gas for a flash loan arbitrage
	randomVariation := uint64(time.Now().UnixNano() % 50000) // Random variation
	result.GasUsed = baseGas + randomVariation

	// Calculate actual profit (in simulation, we'll use a random factor around estimated)
	profitFactor := 0.75 + (float64(time.Now().UnixNano()%5000) / 10000.0) // 0.75 to 1.25
	estProfit, _ := opp.FlashLoanProfitUSD.Float64()
	actualProfit := estProfit * profitFactor
	result.ActualProfit = big.NewFloat(actualProfit)

	return nil
}

// Log the execution result
func (e *TradeExecutor) logExecutionResult(result *ExecutionResult) {
	// Use mutex to protect logging
	e.logger.mu.Lock()
	defer e.logger.mu.Unlock()

	// Determine color and label based on success
	if result.Success {
		if result.SimulationOnly {
			fmt.Println()
			color.New(color.FgHiWhite, color.BgBlue).Printf(
				"[SIMULATION SUCCESS] %s - %s\n",
				result.Opportunity.PairKey,
				result.TxHash.Hex(),
			)
		} else {
			fmt.Println()
			color.New(color.FgHiWhite, color.BgGreen).Printf(
				"[EXECUTION SUCCESS] %s - %s\n",
				result.Opportunity.PairKey,
				result.TxHash.Hex(),
			)
		}

		// Show profit comparison
		actualProfitFloat, _ := result.ActualProfit.Float64()
		estimatedProfitFloat := 0.0

		// Determine which profit estimate to use
		if result.Opportunity.IsFlashLoanViable &&
			result.Opportunity.FlashLoanProfitUSD != nil &&
			result.Opportunity.EstimatedProfit != nil &&
			result.Opportunity.FlashLoanProfitUSD.Cmp(result.Opportunity.EstimatedProfit) > 0 {
			estimatedProfitFloat, _ = result.Opportunity.FlashLoanProfitUSD.Float64()
		} else {
			estimatedProfitFloat, _ = result.Opportunity.EstimatedProfit.Float64()
		}

		// Calculate profit accuracy
		profitAccuracy := (actualProfitFloat / estimatedProfitFloat) * 100

		fmt.Printf("  Execution time: %v | Gas used: %d\n", result.ExecutionTime, result.GasUsed)
		fmt.Printf("  Expected profit: $%.2f | Actual profit: $%.2f (%.1f%% of estimate)\n",
			estimatedProfitFloat, actualProfitFloat, profitAccuracy)

		// Show appropriate execution method
		if result.Opportunity.IsFlashLoanViable &&
			result.Opportunity.FlashLoanProfitUSD != nil &&
			result.Opportunity.EstimatedProfit != nil &&
			result.Opportunity.FlashLoanProfitUSD.Cmp(result.Opportunity.EstimatedProfit) > 0 {
			fmt.Printf("  Method: Flash Loan via %s\n", result.Opportunity.LenderUsed)
		} else {
			fmt.Printf("  Method: Regular Arbitrage\n")
		}
	} else {
		fmt.Println()
		color.New(color.FgHiWhite, color.BgRed).Printf(
			"[EXECUTION FAILED] %s - Error: %s\n",
			result.Opportunity.PairKey,
			result.Error,
		)
		fmt.Printf("  Execution time: %v\n", result.ExecutionTime)
		fmt.Printf("  Mode: %s\n", map[bool]string{true: "Simulation", false: "Real Execution"}[result.SimulationOnly])
	}
}

// Get a summary of execution results
func (e *TradeExecutor) GetExecutionResults() []*ExecutionResult {
	e.resultsMutex.RLock()
	defer e.resultsMutex.RUnlock()

	// Return a copy to avoid race conditions
	results := make([]*ExecutionResult, len(e.executionResults))
	copy(results, e.executionResults)

	return results
}

// SetSimulationMode toggles between simulation and real execution
func (e *TradeExecutor) SetSimulationMode(simulate bool) {
	e.simulationMode = simulate
	if simulate {
		e.logger.Info("Trade executor set to SIMULATION mode - no real transactions will be sent")
	} else {
		e.logger.Warning("Trade executor set to REAL EXECUTION mode - trades will be executed on-chain")
	}
}

func main() {
	// Parse command line flags
	configPath := flag.String("config", "config.json", "Path to configuration file")
	simulationMode := flag.Bool("simulation", false, "Run in simulation mode")
	flag.Parse()

	// Set simulation mode if requested
	if *simulationMode {
		os.Setenv("SIMULATION_MODE", "1")
	}

	// Create monitor service
	service, err := NewMonitorService(*configPath)
	if err != nil {
		log.Fatalf("Failed to create monitor service: %v", err)
	}
	// Start monitor service
	service.Start()

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Block until signal is received
	sig := <-sigCh
	fmt.Printf("Received signal %v, shutting down...\n", sig)

	// Stop monitor service
	service.Stop()
}
